<!-- Copyright The Argmin Authors. -->
<!-- SPDX-License-Identifier: CC-BY-4.0 -->

# Metadata Command Stream

This guide defines the target command-stream coordination model for Phase 9.2
of the multihost transition. It is about runtime ownership and recovery of
metadata mutations. The canonical metadata encoding, state digest, and replay
format are described in [metadata-model.md](metadata-model.md).

## Model

A placement group has one metadata command stream. All metadata mutations for
that PG are ordered by this stream.

The command stream is deliberately single-writer and single-pending:

- a PG has at most one unresolved command slot
- the PG-primary durable state allocates command log indexes
- retries must finish or durably abandon the existing command slot before a
  later command can be issued on that PG
- command application is serialized by durable PG-primary state and SQLite
  transactions, not by process-local locks
- concurrency comes from different PGs, not from concurrent mutations inside
  one PG

This is a correctness choice, not just an implementation shortcut. Allowing
multiple unresolved commands inside one PG would require durable queueing,
gap-handling, and retry ordering. That can be optimized later if needed. The
Phase 9.2 target keeps the unit of ordering small and explicit.

## Command Slots

A command slot is a durable intent for one metadata command on one PG. It is
created on the PG primary before acting-set fanout and remains visible until
the command has reached a terminal durable state. Non-primary replicas must not
store unresolved command slots; they store only accepted command-log entries
and materialized metadata state.

The Phase 9.2 implementation starts with a minimal unresolved-slot row. It
records:

- PG id
- log index
- command id, checksum, and canonical command bytes
- command kind and diagnostic scope, such as bucket, object key, upload id, or
  stream session id where available

The broader target model may add explicit slot state and timestamps if they are
needed for observability, retry ownership, or timeout handling. A terminal
applied or abandoned state is currently represented by the command log itself;
once the terminal log entry is durable, recovery should clean the unresolved
slot row.

An abandoned tombstone uses its independently versioned V2 grammar and persists
SHA-256 of the complete original applied-command bytes, alongside the command
ID and CRC64. CRC64 alone is not exact-source evidence: a different canonical
command can preserve it while changing a reservation proof. Retained log rows,
compacted terminal receipts, pending-slot cleanup and peering comparisons must
all preserve/check the original SHA-256 binding. The receipt hashes the complete
V2 tombstone, so compaction retains that binding without retaining the full
source command. Tombstones remain materialized-state no-ops; this adds no extra
command, transaction or fsync. V1 tombstones are unsupported, not upgraded or
accepted through a CRC-only fallback.

The transitional in-process retry cache must follow the same shape while the
request paths are being moved over: it is keyed by PG, not by bucket or object.
Older helper names may still mention buckets because callers use a bucket to
derive the routed metadata PG, but a pending command for any bucket on that PG
occupies the single stream slot.

While that bridge exists, any bucket-named cleanup helper must still prove slot
ownership before removing it. A cleanup path for bucket `A` may observe that
bucket `B` owns the PG slot so it can drain or wait for that command, but it
must not erase bucket `B`'s recovery state.

Production command-id allocation is no longer process-local. New request-path
commands derive the next log index from the routed PG primary's durable command
log. If the PG primary has an unresolved durable pending slot, request paths
must load and converge that slot before allocating later work. The current
implementation can rehydrate bucket-PG and object-PG command slots from
durable command bytes. It must never skip over an unresolved durable slot. This
means an already-open coordinator handle must allocate after commands appended
by another handle, but must not allocate after unresolved durable intent. Test
fixtures may still use test-only helpers when they manually construct
artificial command envelopes; those helpers are not production ordering
authorities.

Finishing another request's pending slot must not steal ownership of resources
created by that command. In particular, draining a non-matching
`ReserveObjectGeneration` command applies the reservation and leaves it owned by
its original reservation id; the drainer must not release it just because it was
not the current request. Cleanup of genuinely orphaned reservations needs
durable ownership/scavenger semantics, not a best-effort guess by a later
request.

The slot is coordination state, not serving metadata. Request paths must not
derive visible S3 state from a pending slot. Serving state is the materialized
metadata tables whose accepted log prefix and digest agree with the command
stream.

## Normal Flow

For a new metadata mutation:

1. Route the request to the owning PG primary.
2. If the PG has an unresolved command slot, finish or abandon that command
   before starting new work.
3. In a PG-primary transaction, allocate the next log index and insert the new
   pending command slot.
4. After final admission succeeds, durably mark the exact primary pending slot
   as publication-started while holding the primary command section. This mark
   must commit before any witness apply is dispatched.
5. For a redundant acting set, durably apply the
   exact command to the lowest-node-ID non-primary replica. This deterministic
   member is the off-primary publication witness.
6. Apply the exact command to the PG primary. The primary's durable row is the
   serving-publication boundary; the off-primary witness ensures that state
   visible through the primary is already durable in another configured failure
   domain.
7. Apply the exact command to the remaining non-primary acting-set replicas in
   deterministic node-id order.
8. Record the applied command log entry or abandoned tombstone durably on each
   acting-set member as part of that member's atomic metadata transaction.
9. Mark the slot terminal and remove it only after the terminal durable record
   is visible enough for retry/recovery and acting-set convergence has been
   proven, or an explicit command path owns an equivalent cleanup proof.

A failure before publication-start uses the caller's finite work budget. Once
publication-start may have been recorded, publication confirmation uses its own
short absolute deadline and retries only the exact installed command. The
client transport carries a typed `NotSent`, `Definitive`, or `MayHaveApplied`
result from connection admission through authenticated response verification;
coordinators must not infer dispatch from a broad error such as `Io` or
`PayloadDecode`. The one deadline covers admission, connect, write, response,
and exact-state observation, and no new operation may start after it expires.
Once the primary has confirmed publication, a recoverable trailing-replica
failure is handed to the long-lived recovery worker instead of retaining the
request worker. No stage may allocate a replacement command or rerun request
preparation after publication-start. After acting-set application has converged,
transient reservation-release failure is terminal cleanup rather than a failed
mutation outcome. The primary pending slot remains durable so command recovery
can retry replica convergence or release, and is removed only after both
succeed. Applied `AbortStreamUpload` and `AbortMultipartUpload` additionally
require successful deletion of every recorded aborted file and acknowledgement
before slot removal, in both foreground and generic recovery completion.
Applied `CommitStreamPart` likewise retains the slot until every displaced
part file and acknowledgement is deleted; the newly published part and other
completed parts remain untouched. Reservation-release failure must not skip
those independent physical cleanup attempts. The exact pending command remains
retryable on failure. `CommitMultipartObject` retains its existing best-effort
physical cleanup policy; it is not upgraded by these rules.

### Publication And Retry Boundary

Witness-then-primary fanout is a deliberate protocol contract, not merely an
iteration order. A request remains **abortable** only until final admission has
succeeded and before the primary pending slot is durably marked
publication-started. Bounded contention or an authoritative rejection in that
state may return `SlowDown`, and a command that provably did not cross that
marker may be abandoned according to its command-family rules. Snapshot-sensitive
conditional mutations must distinguish that safe abandonment from uncertain
publication: while their request budget remains, they discard the stale computed
result and re-evaluate the condition against a fresh snapshot. If a conditional
PutObject has already encountered and safely abandoned an overlapping command
when the budget expires, it returns `ConditionalRequestConflict` without starting
a post-deadline snapshot read. The reinspection read itself uses that absolute
deadline for local PG acquisition and remote transport; deadline exhaustion maps
to the same conflict outcome. Other repeated, definitively unpublished contention
may return `SlowDown` when that outer budget is exhausted. Direct and streamed
PutObject and conditional delete-marker exact
replays, pending-command drains, waiter observations, pending-slot installs, and
stale-snapshot retries consume this same outer operation budget; they must not use
a shorter or fresh nested budget that can expose internal PG churn as `SlowDown`
before the condition is re-evaluated or extend work beyond the operation deadline.
Initial acting-set abandonment observation and pending-slot installation both
consume the same operation deadline. Bucket, object, and direct-PUT finishers use
one observation policy: retryable local, route, convergence, and transport failures
retry within that budget, then receive exact publication classification on exhaustion;
fatal integrity and protocol failures still fail closed. Pending-slot installation checks
the deadline while holding the frontend PG lock,
and remote requests carry the same bound to a storage-side check under the storage
PG lock, so an RPC that times out cannot install its slot later. This includes
bucket-control slot installation for versioning, ACL, property, and subresource
commands. The portable operation deadline is a conservative wall-clock upper
bound with the supported cross-process skew removed. Unix and TCP RPC use the
same representation and never compare monotonic timestamps from different
processes. The receiver binds the wall deadline once to its own monotonic clock;
all subsequent admission, lock waiting, and mutation checks use that local
deadline. The storage worker derives both command-admission and PG-lock
deadlines from that bound and retains the post-lock validity check. Insert
transport outcomes remain
typed as `NotSent`, `Definitive`, or `MayHaveApplied`: definitive budget
exhaustion releases request-owned reservation authority, while ambiguous
dispatch retains it for exact-command recovery. If a lost insert response is
followed by recovery applying and clearing the command, a same-index conflict is
certified against exact terminal rows and resumes that command's outcome; a
different checksum, bytes, or hash chain remains a fatal conflict. Transient
exact-command drain and waiter-observation failures retry within the shared
budget; integrity and protocol failures still fail closed.

Checkpoint compaction may delete full terminal command rows, but it must first
persist an exact terminal receipt containing the command checksum, SHA-256,
terminal disposition, and hash-chain links. Pending-slot insertion, exact
terminal inspection, and replay consult this receipt. A compacted exact replay
therefore returns the original terminal state; a same-index command with
different bytes or hashes remains a fatal conflict. Receipts retain a fixed
per-epoch replay window. The checkpoint catalogue remains the compacted-through
watermark, so a command older than that window is rejected fail-closed and can
never be reinstalled; it is not reported as an exact replay once its receipt has
aged out.

Historical source inspection validates a contiguous hash chain using full entries
or retained terminal receipts. When a checkpoint at or after the source exists,
it validates the source-to-checkpoint suffix against the nearest such verified
checkpoint's prefix hash; the pruned earlier prefix is not required. Otherwise
it validates forward from the preceding checkpoint/genesis base. The exact
source checksum/SHA-256 must match in either direction. Gaps between source and
anchor, invalid links, or a chain that does not match the checkpoint fail closed.
Recovery custody must preserve the evidence it still needs; this does not extend
the bounded receipt-retention window or recover a pruned source from a checkpoint
alone.

The request enters **irrevocable convergence** as soon as either:

- the exact primary pending slot is durably marked publication-started;
- any acting-set member is known to have accepted, mutated, or logged the exact
  command; or
- a dispatched apply has an ambiguous transport outcome, so the caller cannot
  prove that no durable apply occurred.

The publication-start marker is durable intent, not a serving-publication or
success witness. It closes the interval in which a witness RPC has been
accepted by the server but has not committed yet: a replacement owner that
acquires the primary section must reconstruct the marker and retain the exact
command even when no actor row is currently observable. Abandonment and reissue
must reject the marked slot. The S3 success boundary remains confirmed exact
rows on both the deterministic witness and the primary.

Standalone restart must also treat a marked primary pending slot as
irrevocable even when every actor row is still at the preceding log index. It
must converge that exact command without revalidating an expired command-owned
reservation, then perform terminal reservation release and pending-slot
cleanup. An unmarked command with no applied actor row remains abortable and
must pass ordinary admission validation before its first apply.

Post-budget publication classification must observe the exact primary pending
slot while holding the command-bound primary section. If the section, marker,
or actor state cannot be observed before the shared absolute deadline, the
classifier retains irrevocable uncertainty; it must not infer `NotPublished`
from the absence of an actor row and return caller-visible `SlowDown`.

After that boundary the implementation must converge the same canonical
command and hash-chain position. It must not abandon or replace the command,
rerun request preparation, or turn request-work-budget exhaustion into a
caller-visible `SlowDown`. The exact durable row already present on the witness
proves the original admission for the primary and later replicas even if the
original bucket-write reservation expires during convergence. Divergent
same-index state remains a fatal fail-closed condition; it is not retryable
contention.

Normal reads may observe the primary's published mutation while the originating
request is still converging trailing replicas. The S3 success response may be
published once both the off-primary witness and primary apply are confirmed.
The request does not wait indefinitely for every trailing replica: a
recoverable trailing failure retains the exact primary pending slot and hands
convergence to the replicated-mode recovery worker. Heartbeat discovery fences
the PG in Peering, and the refresh worker uses the exact command identity plus
the process-wide per-command recovery flight to deduplicate convergence. The
pending slot and bucket-write reservation remain durable until that worker has
converged every required replica and completed terminal cleanup.

The recovery flight preserves whether publication succeeded but trailing
replica convergence or terminal cleanup remains pending. An exact semantic
retry may project its committed result from that recorded outcome without
waiting for cleanup. A different foreground operation must atomically request
the authorized-recovery handoff and return retryable contention promptly; it
must not spend its request budget retrying another operation's retained slot.
This policy applies equally to a caller that arrives after handoff and one that
was already waiting when the owner records the nonterminal outcome.

Failure of the frontend runtime-map refresh requests one fallback current-map
scan at the start of that failure episode. Repeated refresh failures do not
request another full-PG scan; a failed fallback retries on a bounded cooldown,
while exact control-plane recovery listings continue to be polled normally.
Targeted work, discovery failures, and listing transport failures do not consume
the fallback request: only an attempted full current-map scan advances its
outage generation and starts or clears its retry cooldown.
This prevents route unavailability from amplifying into continuous all-PG
storage RPC traffic.

Standalone static route maps have no control-plane refresh worker. A shared
storage-owned static recovery worker therefore performs the same bounded
current-map scan periodically. It reads the exact durable pending envelope from
the trusted current primary and supplies that envelope as the authorized
recovery source, allowing it to take over a detached foreground flight. An
unrelated foreground request still cannot mint that authority and continues to
return retryable contention promptly. This prevents a recoverable trailing
replica or terminal-cleanup failure from poisoning the PG until process restart.
The dynamic refresh-outage fallback remains deliberately non-authorizing:
dynamic recovery authority comes only from the control plane's PG-scoped
listing. Both scans attempt every PG, retain the first error for their caller,
and continue converging later PGs so one broken route cannot starve unrelated
pending commands.

Cross-PG reservation routes are stored in a bounded sidecar in the same
transaction as the pending slot. Its integrity checksum binds the complete
command ID, command CRC64, command SHA-256, and every retained route dependency, so heartbeat
construction does not read, allocate, hash, or decode the command envelope.
Schema 13 additionally stores the mandatory 32-byte command digest and uses up to five tagged 17-byte dependency records (bucket-route hash
or exact PG), ordered without duplicates. Exact PGs are not rehashed and must
belong to the configured topology. Current commands still project only their
ordinary reservation; the new owner-window descriptors and their exact pending
dependencies are not yet wired into production publication.
The same pending-slot transaction also initializes a bounded staging-protection
index, currently explicitly empty for production commands. Its private nonempty
writer consumes command-bound introduction/lookahead projection, checks the exact
pending envelope and topology, and retains shard-index-ordered file owners and
the independent acknowledgement owner. Scavenger paging consumes it as exact
protection-only evidence, with a cursor bound to the slot, index checksum and
topology; changed slots restart discovery, and malformed indexes fail closed.
The audit pass enforces native phase/key progress across responses. Only a
pending-slot cursor permits a restart; the retired slot/index identity cannot
reappear, and at most eight replacements may restart one PG's scan. Cycles or
exhausted restart budgets invalidate the pass before physical auditing. The
maintenance worker discards partial references and prior absence evidence,
backs off, then starts fresh discovery rather than treating truncation as
completion. Progress history retains only the bounded retired pending identities,
not every scanned key.
It conveys neither readable payload nor physical authority. Production nonempty
descriptor dispatch and frontend custody activation are still pending.
The owner-local bound pending installer derives route/window indexes from the
immutable command/descriptor before entry. Session-backed placed/reclaim pages
are derived inside the existing pending transaction, so they cannot reuse a
pre-transaction session view. Fresh append/nonempty-abort protection requires the
exact bucket/key/session encryption row; a missing or crossed row is not an empty
inventory. Multipart terminal commands must supply one subject-matching header
for every stream segment, without duplicate session headers. Empty aborts need
no session geometry. This strict projection is shared with complete-context
inspection; source-bearing abandonment retains its separate original-source
geometry. Projection failure rejects a fresh installation before its first slot
write, while an exact existing slot (or failed inspection) keeps possible-
acceptance classification and is never repaired. Applied replay remains separate
and does not require a session that terminal application already removed.
Unpaged discovery of an already-installed command has a different obligation:
an append/nonempty abort continues to protect each exact shard-set identity even
after the live session disappears. Only an exact bucket/key/session lookup may
supply placed geometry and its command-carried integrity witness. Missing or
crossed live subjects yield protection-only references, not empty discovery or
invented backfill authority; malformed encryption or incomplete terminal stream
headers fail the entire scan closed. This uses the existing read snapshot and
does not rewrite the pending indexes or grant cleanup authority.
The slot, pages and indexes are installed together;
no extra transaction or successful-write durability barrier is introduced.
It cannot pair a different command with that projection. Exact
retries, including ordinary commands with an empty staging projection, compare
the retained route/window evidence and both payload-page indexes without patching
them. Payload validation derives expected pages from the bound command under the
same transaction and compares both slot counts, every page index/count and exact
encoded bytes, including absence of extra rows for an empty index. Blob reads
are bounded before materialization; missing/corrupt rows are not repaired by an
exact retry. The empty staging row
must exist with the exact command-bound checksum; a missing row is not equivalent
to an empty projection. A known exact slot with damaged or mismatched evidence
retains possible-acceptance
classification. A nested insertion uses a savepoint so an index failure cannot
leave a prefix that its caller could commit. This plumbing does not yet activate
nonempty descriptors in command/RPC dispatch or authenticate the full future
descriptor carrier; the bounded indexes are inspection projections, not that
carrier or physical authority.
Occupied-slot same-payload reissue uses the same projection transaction. A bound
successor borrows the original immutable description rather than accepting a new
window or cutoff. Replacement validates the observed source and its indexes
under SQL serialization, derives replacement payload pages in that same
transaction, keeps the publication-start guard, and updates the slot,
reference pages and both indexes together. Nested replacement uses a savepoint;
index failure cannot leave a partially replaced slot for an outer commit.
Exact replacement replay validates the successor indexes too, and retains
`MayHaveApplied` classification if that evidence is missing or corrupt. Ordinary
replacement cannot erase a nonempty bound projection. This is owner-local
same-epoch integration plumbing, not activation of descriptor-bearing recovery
RPCs or cleanup-derivative handoffs.
The preparatory fresh-introduction capture entry derives its subject from the
exact reserve/create command under the primary's enclosing metadata snapshot.
It rejects occupied attempts, stale generation/command candidates and crossed
stream authority, and loads the enclosing MPU generation itself. Capture is
read-only and does not authorize acceptance or physical writes. Production
builder wiring, mandatory descriptor carriers and frontend custody remain part
of the coordinated activation; retries must preserve, not recapture, descriptors.
Fresh append capture likewise loads its retained owner under the primary snapshot
and requires its prepared, unselected tail, exact file/ack placement, live
responsibility and original issuer. It binds the derived lookahead to the next
command without writing state. Replica apply retains its distinct replay rules;
it does not acquire local preparation authority, and a restarted primary cannot
recapture an old descriptor. Production dispatch is not yet switched to these
capture components.
The slot remains the durable cleanup owner: terminal cleanup releases every
command-owned reservation before removing the exact pending slot.

The candidate complete publication-start component preserves the existing
trial-and-marker transaction protocol. It validates the exact pending carrier,
scope and indexes, applies the complete transition under a savepoint, rolls it
back, then commits the marker. An opaque admitted-primary authority retains the
command, locked store, serialization section, route lease and work deadline;
effect/commit checks cannot be replaced with a caller-provided callback. Applied
replay skips trial effects. Failed rollback retires the connection, and every
failure leaves publication custody unresolved rather than licensing cleanup.
Current publication requires the Active primary. Candidate historical publication,
over Unix/TLS or embedded storage, instead requires the current Peering map's
exact recovery report and retained Active-primary route. The embedded entry
authenticates the supplied report snapshot against the dynamic admission's full
map-content identity and selects the runtime from that admission, not from a
caller-supplied node or old map. Its shared PG serialization scope retains the
reported command epoch/index/CRC64/complete-command SHA-256 and a bounded current-map fence. Same-epoch
renewal cannot extend that captured fence. The report grants admission only:
the store still checks exact complete command/context bytes or applied evidence.
Only owner tests select these candidate components; live publisher selection,
frontend custody and end-to-end recovery handoffs remain gated. No additional
ordinary-path transaction or fsync is introduced.

The candidate current-replica application receiver similarly requires opaque
authority bound to the exact complete command, configured topology, locked store,
Active admission domain and server PG serialization registry. Primary-session
locks must also match the current PG/epoch. The captured route/work cutoff bounds
lock waits and is rechecked before the first transactional mutation and before
commit. Exact replay validates the complete terminal identity without repeating
owner transfers or log mutations. The raw apply adapter and failure callbacks
are unit-test-only; production callers cannot substitute independently supplied
commands or a mutation callback. This component adds no transaction or fsync.
The installed candidate Unix/TLS receiver also accepts exact report-authorized
historical application. It binds the report's PG/epoch/index/CRC64/complete-command SHA-256, requires
its reporter to be the retained Active primary, and validates historical member
and Peering placement. The reporting primary must hold its exact recovery session;
replicas serialize locally. A bounded current-map fence, intersected with the
immutable work cutoff, is rechecked at the existing effect/commit boundaries.
Lost replies keep the original holder uncertain; exact retry and reopen validate
complete identity without repeating mutation. Historical embedded selection and
mixed-cohort fanout and quiescent slot retirement are described below;
other non-quiescent terminal recovery transfers remain a separate gate; independent
command/source reservation release is described below.
The candidate embedded receiver instead borrows an exact current acting-set
target from its admitted primary section, keeping that section's shared PG guard
alive through application. It never recursively locks the same PG for a replica.
The private node owner selects the embedded runtime/store and binds the complete
identity, topology and original route/section/request cutoffs; the same transaction
rechecks them before fresh effects and commit, including replay. Nonmembers cannot
mint targets; historical sections require the separately report-authorized
selection described below. The owning candidate fanout holder
now selects this embedded receiver only through a primary section from its exact
original admission/PG/epoch. It borrows the retained command and first cutoff
without a wire round-trip or fresh submission, records uncertainty before local
application, and preserves prior confirmation across failed retries. The same
ordering rule serves embedded and remote members: witness before primary, primary
before trailing replicas, with all-member evidence required for terminal handoff.
Handoff retains the same command and installation/application histories; neither
failed handoff nor Drop manufactures cleanup or retirement. Current embedded slot
retirement and historical fanout/quiescent handoff are described below;
remaining non-quiescent historical terminal work and live frontend selection remain gated.
An owner-test-selected Unix/TLS receiver now carries the complete context and
an absolute conservative wall cutoff, rebound once before context decoding.
It returns an application-specific, exact-command/state confirmation only after
the admitted apply transaction. Restart and lost-reply retry preserve no-write
replay. The candidate owning publication holder now sends current-replica
application through closed Unix/TLS operations, including on its held primary
session. Application requires confirmed publication; the original complete
context, installation/publication history and first cutoff remain in that same
holder. Each target independently retains unconfirmed or confirmed application
history across explicit retries. Not-sent attempts cannot erase earlier
uncertainty, and later errors cannot erase exact application evidence. These
are per-peer observations, not an acting-set completion or retirement capability.
A consuming pre-publication bind now captures the entire Active acting set from
the original route admission, narrowing the holder's frozen cutoff if necessary.
The bound sender accepts only the exact registered clients and the primary's
own session, not another client with matching node/endpoint fields. It requires
the lowest-ID non-primary witness confirmation before primary application and
primary confirmation before trailing application. All-member completion evidence
borrows this holder and its admission; a quorum or ambiguous reply is insufficient.
Captured route expiry invalidates that evidence even after same-epoch map renewal.
This candidate evidence still cannot remove a slot, release resources or delete
payload. A consuming terminal handoff now retains the same holder and all its
history, returning that holder on incomplete or expired completion. It derives
scalar work diagnostics from the exact complete context, separating command-proof
release, command payload cleanup, durable reclaim handoff and the additional
retained finalizer proof of a source-bound abort. Those diagnostics are not
resource authority or evidence of cleanup; an empty observation does not mean
the durable owner has no outstanding work. Neither handoff nor Drop performs
effects. Its closed command-proof release operation now retains that context,
binds release to its complete SHA-256 identity, and derives the exact proof and
historical bucket route internally. The first local/absolute-wire cutoff cannot
be renewed on retries. Entered attempts remain unconfirmed after error or unwind;
only exact success/absence confirms release, and an already confirmed release is
not dispatched again. Live PUT-stream creation proofs remain retained. This is
not a release of every durable-owner resource. The additional finalizer in a
source-bound UploadPart abort now has a separate closed release and observation,
derived from the authenticated source retained inside the complete context. It
requires all-member cleanup application, never a tombstone alone or a missing
session. Ordinary aborts have no such release. The source's original historical
bucket route and first cutoff survive cleanup reissue and explicit retry; neither
proof's confirmation changes the other's history. Durable release-due evidence
remains conservative for later owner transitions. Described Direct PUT and other
retained resources still need their respective owner protocol.
The local exact-slot retirement prerequisite now requires both that all-member
terminal holder and admitted primary authority. Its direct invocation remains
test-only. It accepts commands with no immediate
payload/release/reclaim work, and the separately checked UploadPart creation
handoff below; it validates exact Applied evidence and all pending
indexes/scope, and retains uncertainty across failed deletion/commit confirmation.
Slot retirement leaves every materialized owner obligation intact; zero payload
counts alone never grant it. The closed sender now lends an operation-specific
retirement attempt to the original registered primary and held session, preserving
the first cutoff and uncertainty history. Purpose-tagged confirmation binds the
complete command SHA-256 and exact node/position; later unsent attempts cannot
erase uncertainty, and confirmed retirement does not redispatch. The candidate
Unix/TLS receiver now independently validates current Active-primary admission,
the exact session/lock domain, and complete-context eligibility. Its pre-lock
captured server route fence and receiver-bound wire/work cutoff are rechecked
before DELETE and COMMIT. Exact absence with authenticated terminal evidence and
no orphan indexes confirms without repeated mutation, including after restart and
compaction. Peer completion remains the owning sender's guarantee, not a certificate
inferred from wire framing. The same owning terminal holder now selects closed
embedded retirement using a primary section from its exact original admission,
PG and epoch. A private node owner locks the selected store and combines its
publication authority with the stack-only all-member terminal permit; neither is
sufficient alone. Original route/work and narrower attempt/section cutoffs reach
the existing pre-DELETE/pre-COMMIT checks, also for absence confirmation. Both
transports share the owning attempt's uncertainty and confirmed-retirement history;
errors/unwind cannot detach custody, and confirmed retirement never redispatches
against a later slot. No new encoding, transaction or fsync is added.

UploadPart creation additionally requires the sender's confirmed exact creation
reservation release. The receiver independently checks the retained immutable
introduction, initial window/participants, exact creation `ReleaseDue` proof and
generation high-water fence inside the same deletion transaction, including an
absent-slot retry. Bounded indexed reads avoid scanning later append/finalizer
history. Owner and proof rows survive removal: this is pending-slot retirement,
not cleanup completion or session continuity. A claimed remote release is not
accepted as authority to discard durable responsibility. Other non-quiescent
terminal cases remain gated, as does mandatory live selection.

For introduction/append contexts with a future window, the retired terminal
holder can now be consumed into a confirmed-window holder through a fresh
embedded or Unix/TLS primary/witness inspection. It retains the exact complete
context, all installation/application/release/retirement history and original
admission. The confirmation attempt cannot extend the publication or immutable
window cutoff. Failure returns the same terminal custody; a lost reply requires
explicit fresh confirmation, never a synthesized positive observation. Success
has no reverse conversion, raw descriptor extraction or physical-effect API.
Embedded streamed PUT/UploadPart can consume it through the candidate one-slot
allocator: the existing cursor transaction revalidates the original prefix,
window, issuer/session/generation and cutoffs under primary/PG serialization.
The resulting allocation or failure keeps the complete custody and has no
reset/retry conversion. An ambiguous commit never yields a grant. This adds no
ordinary transaction/fsync or durable/wire format change. Direct PUT and remote
allocation, guarded physical effects and mandatory live selection remain gated.

Owner tests
also exercise this handoff with an embedded primary and simultaneous authenticated
Unix/TLS peers, including each witness transport, lost peer/primary confirmations,
no-write exact retries and frozen-route expiry. Remote-primary/embedded-replica
application now uses a closed target derived from the original admitted cohort,
exact registered primary client and live borrowed session. The session borrow
excludes concurrent local transport/shutdown through the replica transaction;
failed exchanges and peer-declared closure invalidate further use. It is not a
remote-liveness guarantee
or a substitute for exact primary confirmation. Both directions retain the same
command holder, cutoff, ordering and uncertainty evidence. Historical embedded
application now borrows the report-authorized embedded primary section. Its
targets require old Active membership and current or retained Peering placement;
the exact reported complete SHA-256 is rechecked before store acquisition and at
transactional effect/commit, including replay. It retains the same section,
admission and bounded cutoffs, with no additional transaction or fsync. A separate
historical remote-primary route now admits embedded replicas using the same
authenticated report and placement rules. The exact registered primary client
and its live historical session are borrowed through the transaction; acquisition
is deadline-aware and failed exchanges revoke that session. The admitted route
selects the historical session epoch on the registered current-epoch client;
the raw epoch-selecting opener is private. Report SHA-256,
route, target and original work cutoffs are enforced before store access and at
effect/commit. This receiver-only route cannot mint fanout completion or slot
retirement.

Historical fanout now binds the owning sender to the
complete original Active cohort, not merely the subset eligible for individual
receiver admission. Every original member must have current or retained Peering
placement; missing members reject before binding or effects. Embedded, Unix and
TLS endpoints can share a cohort under either an embedded or remote primary.
Remote sends require the exact clients registered in the admitted map; local
shadow stores cannot substitute. Historical replica transport additionally checks
the opaque cohort's exact report before allowing an old command epoch on that
current authenticated client; ordinary sends keep their epoch equality check.
Binding checks the report's exact command
ID, CRC64 and complete SHA-256 and retains the original installation history and
work cutoff. Application requires the primary section from that exact admission
and report. The canonical witness precedes primary application, which precedes
trailing replicas; lost confirmation requires exact retry, including after
restart. The owning sender opens the historical primary session at its reported
command epoch through the current registered client, with unchanged current
authentication. Opening/reopening intersects the attempt, frozen work and
captured route cutoffs; raw epoch selection remains client-private. Remote-primary
embedded targets recheck the report's complete identity before store access and
at effect/commit. Session borrowing and deadline-bearing sends use deadline-aware
mutex acquisition; immutable PG identity checks cannot block ahead of that wait.
Not-sent timeout preserves prior uncertainty and does not revoke another holder's
session. Historical all-member completion can now be consumed into a terminal
holder for a quiescent complete context or independent command/source reservation
release. Other commands with immediate payload or reclaim work still return their
original holder. Exact slot retirement
uses the original primary section, with no context reconstruction or history
reset. Remote admission independently binds the exact historical primary/report
(including complete SHA-256), local Active gate and held lock/domain. The
pre-wait captured route fence and original work cutoff remain effective at DELETE
and COMMIT even after raw renewal. Embedded retirement keeps its report-bound
section through the same checks. Lost replies preserve uncertainty; exact
absence can confirm without writes, while a confirmed holder never removes a
later slot. Materialized owners, references and resource obligations survive
retirement unchanged; it grants no physical cleanup permission.
The closed releases select either the command's exact proof or the source-bound
abandoned finalizer's separate proof, through that proof's original bucket-PG/epoch
route, even when a different node is
the current bucket primary. Production route-reference discovery supplies that
exact epoch to storage-node refresh. It retains the original complete context,
source, uncertainty and cutoffs across loss/retry; SQL rechecks the cutoff beside
DELETE. Exact absence confirms an uncertain release without further writes,
including after restart from the retained pending context. PUT stream creation
keeps its live proof; ordinary UploadPart abort supplies no command proof. Durable resource evidence,
physical work and pending-slot ownership remain untouched; even successful proof
release does not make this non-quiescent context eligible for slot retirement.
Owner tests alone select these paths; live dispatch still rejects this grammar.

The candidate primary also has a consuming unstarted-abandonment holder for
embedded and authenticated Unix/TLS execution.
It retains the same complete context/source, installation history, admitted
cohort and first work/route cutoffs; it cannot be converted back into a fresh
publication submission. Observed/uncertain publication or application prevents
this handoff. Under the original primary serialization, the transaction requires
the exact pending bytes, indexes and bucket scope, an unset publication marker,
and no applied terminal evidence. It records the SHA-256-bound tombstone without
applying the command, deleting the pending slot, transferring resource custody
or deleting payload. A confirmed existing tombstone is a no-write retry; errors,
unwind and lost confirmation preserve uncertainty. Failed rollback retires the
connection. Historical admission requires the exact control-plane report and
original primary, not current-map substitution. This is **primary-only**: it is
not all-replica abandonment, cleanup/follow-up authority or epoch reissue.
The closed remote attempt retains that same holder across session reopening,
not-sent failure, lost reply and exact retry. Its purpose-tagged request carries
the complete context and the first conservative absolute wall cutoff; the
receiver binds it to monotonic time before decoding and intersects its pre-wait
route fence. Receiver-owned authority independently requires Active admission in
its own domain and the exact held current/historical primary lock/report. That
authority reaches the common transaction and final INSERT/COMMIT checks. A
confirmation binds node, command ID and complete SHA-256; even an authenticated
crossed confirmation cannot resolve uncertainty. Only owner tests select this
receiver; the live predecessor decoder still rejects the candidate grammar.

After primary tombstone confirmation, that same holder can now record tombstones
on **embedded** replicas. A private borrowed permit binds the exact complete
context, replica and original admitted primary section or Unix/TLS primary
session. It cannot be minted from a raw target or an ordinary publication permit.
The receiver opens its own PG under this retained serialization and rechecks the
captured cutoffs beside INSERT and COMMIT. A replica may have no pending slot;
an existing slot must match exact bytes, scope, indexes and unset publication
marker. Orphan indexes, crossed terminal identity and applied state fail closed.
Exact tombstones confirm without writes. Per-replica uncertainty survives errors,
unwind and lost replies; restart rebuilds confirmation by exact reinspection.
Neither a single replica confirmation nor the complete set grants cleanup,
resource release, source extraction or pending-slot retirement. Follow-up
construction and its durable custody transfer are still gated.

Owner tests now exercise the remote replica **receiver** over authenticated
Unix/TLS. Its separate `RABD`/`RAOK` V1 purpose binds the configured primary,
recipient, complete command and mandatory conservative absolute cutoff. It
requires its own Active admission/domain, a distinct current or exact
report-authorized historical replica, and locally acquired PG serialization.
It never borrows a held primary session. The captured server route fence and
rebound wire cutoff reach the same replica transaction and final INSERT/COMMIT
checks; renewal cannot extend either. Acknowledgement is emitted only after
commit and binds both nodes, command ID and complete SHA-256. Replica restart
replays exact tombstones without writes. Primary confirmation remains a sender
custody obligation, not a fact certified by these wire fields. Only test-only
dispatch selects this receiver; live production selection remains disabled.

The same primary-confirmed holder now owns remote replica attempts. Each attempt
borrows its original context/history and either the exact embedded primary
section or the exact registered Unix/TLS primary session. The remote session's
mutex remains held through the replica exchange, with acquisition bounded by
the attempt, original work and admitted route cutoffs. Encoding requires this
attempt's own transport, not a separately decoded identical command with a new
deadline. The recipient must be the exact client registered in the captured
current/historical cohort. Only an exact purpose/primary/recipient/ID/SHA reply
confirms that member. Not-sent failures restore its previous observation;
ambiguous transport, bad replies, remote rejection and unwind leave it uncertain.
Confirmed members are not redispatched, and other members' history is unchanged.
Restart reauthenticates the retained primary and independently reconfirms exact
replica tombstones without replaying writes. This grants no all-member cleanup
or terminal/follow-up capability and does not select live publication paths.

The holder now has a consuming **all-member abandonment handoff**. It checks the
original immutable authority and requires primary confirmation plus every exact
replica in its captured cohort, including historical cohorts. Quorum, duplicated
confirmation, or a committed tombstone whose reply was lost is insufficient.
Failure returns the same owning holder with all observations intact. Success
moves that entire holder into a private, non-cloneable representation; it does
not reconstruct a submission, expose a source, renew authority, or accept an
independently supplied command. Only scalar observations remain available. This
no-effect handoff is the prerequisite for admitted follow-up preparation, not
permission to delete or release anything.

For an abandoned UploadPart finalizer, the handoff can now consume into an
owning, unsent cleanup pair under its exact **current embedded-primary** section.
One read snapshot revalidates the complete source, unstarted pending slot and
protection indexes, authenticates its tombstone, captures current owner/session
state, and derives the successor position from that same log prefix. The original
deadline is checked again after snapshot teardown. Failure returns the unchanged
handoff; success keeps its source, proof responsibility and installation history
private beside the source-bearing cleanup candidate. No installable context,
fresh submission or cleanup right escapes. Historical source authority is
explicitly insufficient to mint a successor: separately admitted historical
transfer and remote-primary preparation remain gated.

That same owning pair can now replace its source slot under the original current
embedded-primary section. One write transaction reauthenticates the unstarted
source tombstone and all pending indexes, requires the captured log prefix, and
recaptures the exact owner/session cleanup closure before replacing the slot and
its protection indexes together. Admission and the immutable work deadline are
checked immediately before the first write and COMMIT. The private attempt
borrows the pair's installation history exclusively: an unconfirmed attempt's
entered error or unwind leaves it unconfirmed, including a lost successful reply;
failed preflight never resets earlier uncertainty or confirmation. Once confirmed,
retry validates the supplied authority and returns without redispatching. This
records past installation, not continued slot ownership; later transitions must
validate their own prerequisites. Unconfirmed exact cleanup replay validates
scope and every index without rewriting a started/applied command. The source,
candidate and proof responsibility remain inseparable. Replacement does not apply
the cleanup, release resources, authorize shard deletion or select the live
publisher path.

After confirmed installation, this same pair can publish the cleanup under its
current embedded-primary section. A private borrowed permit derives the exact
source-bearing request and **borrows the original source's frozen deadline**;
the bridge accepts no independently supplied context or original request. The
lower boundary checks the cleanup identity and deadline-owner identity as well
as the admitted domain/store/primary. The existing publication transaction
revalidates the slot/indexes, trial-applies and rolls back cleanup, then commits
only the publication marker. A lost reply or unwind retains publication
uncertainty without changing installation confirmation or source history.
Confirmed publication is not redispatched, but still requires valid supplied
authority. Uncertain applied replay authenticates the retained complete identity
without repeating proof/resource transfers. Publication confirmation alone does
not grant terminal transfer or physical cleanup.

The pair also owns closed application to its original embedded/Unix/TLS cohort,
under the current embedded primary. The
supplied section must match the original admission domain; targets come from
that section, and requests borrow the same source-bearing cleanup and frozen
deadline. The lowest-ID non-primary witness must confirm before primary apply;
the primary must confirm before trailing members. Each peer's entered error or
unwind remains unconfirmed, including a committed lost reply, without changing
the original source, installation or publication histories. Confirmed application
cannot regress on failed exact retries. Shared replica apply authenticates the
source and atomically transfers session/proof responsibility; exact replay does
not repeat those effects. These per-peer observations are not terminal authority:
the primary slot remains installed and the pair has no cleanup Drop.
Remote replica sends validate the original registered client
identity, retain both commands throughout transport and authenticate the reply's
node, command ID, complete SHA-256 and applied prefix. A definitive unsent attempt
restores its prior observation; ambiguous transport, malformed authenticated
replies and unwind retain uncertainty. No automatic retry or renewed cutoff is
introduced.

After confirmed installation/publication and exact application on every original
member, a consuming handoff moves the entire pair into an opaque abandoned-part
terminal owner. It neither re-encodes a command nor reopens transport. Failure
returns the same prepared pair with all histories intact. The terminal owner
retains both source and cleanup, the original cohort/cutoff and the source's
installation history; scalar work describes outstanding obligations, not cleanup
success. Even an empty cleanup retains its finalizer-proof responsibility.
There is no reverse conversion or command/ordinary-sender extraction, and neither
handoff nor Drop releases a resource or removes a pending slot. The terminal pair
now has a closed retained-finalizer reservation release: a private borrowed
permit binds the exact complete cleanup (including its original source), admitted
route and first local/wire cutoff to the existing proof-release protocol. It
accepts no caller-selected proof, target, deadline or callback. Errors and unwind
after entry retain `Unconfirmed`; authenticated success records `Released`, which
cannot regress or redispatch. An exact absence retry confirms a lost release
reply without repeating the mutation. This releases only the source finalizer's
bucket reservation, not the retained owner-resource row, payload, generation or
pending slot; the pair remains non-quiescent. Non-quiescent retirement for this
pair, remote-primary/historical cleanup preparation/publication and live selection
remain gated.

The terminal pair can inspect local retained custody through its exact admitted
embedded-primary section. One read snapshot authenticates the complete cleanup's
applied identity and original source abandonment, validates the terminal owner,
retained windows, generation high-water fence and exact finalizer obligation, and
rejects surviving live session/generation state. Every unpublished abort manifest
must bind every selected window plus exactly one unused lookahead; an empty
manifest therefore permits only one retained window. For proof-only cleanup of an
already-published owner, the retained window/reference projection must match the
authenticated source finalizer's complete segment manifest, including cardinality,
sizes and placement geometry. Legitimate Part/Object/Reclaim destinations remain
intact; structural agreement between corrupted windows and references is not
sufficient. The
result contains scalar counts only, not a cached validation certificate or
permission to delete, release resources, or retire a pending slot. Original
authority is rechecked after inspection; no snapshot, raw context, inventory or
callback escapes. Read-snapshot teardown uses the existing connection-retirement
protection. The physical-exclusion/retirement gates are unchanged.

The candidate bounded local settled-prefix reader reuses only a connection-local
entry minted by complete startup validation. It binds the PG/topology, replica
prefix, live store identity and SQLite local/external/schema change stamps.
Startup rejects foreign-PG log rows, receipts and checkpoints across all retained
epochs before minting that entry. This subject validation may scan retained keys;
database changes invalidate the entry, and hot lookup never repeats that scan.
Lookup holds a read-only snapshot and rejects pending custody or log, receipt or
checkpoint evidence beyond the stored epoch/position, including future epochs.
Cold or changed state requires explicit validation, never implicit repair
or a checkpoint scan. Closed same-epoch complete apply can advance an existing
validated entry through its own transaction; cold state and epoch transitions
still require full validation. The application implementation is nested under
the private cache owner, with no caller-supplied refresh callback or "mark clean"
API. It consumes the old entry before effects, captures the successor before
commit, and retains it only if the original stamp still matches afterward.
Errors, rollback/unwind, intervening writes and uncertain teardown cannot bless
transaction-local state. Exact replay never promotes a cold entry. Other SQL
writes still invalidate validation. Pending-slot/retirement advancement and
primary/witness admission remain prerequisites before hot-path selection. Its
local prefix observation is not a write grant, global settlement certificate or
cleanup authority, and introduces no new ordinary-path transaction/fsync.

Physical cleanup, non-quiescent/owner retirement,
remaining non-quiescent historical terminal work, reissue and durable follow-up
custody transfer, frontend payload/proof custody
and the live switch remain open.

Heartbeat collection observes PGs independently, so a slot insertion and a
reservation release cannot be captured atomically across PGs. The control plane
therefore persists references omitted from one heartbeat as retiring references
for one further heartbeat. Current and retiring references both protect route
history and are included in node refresh. A later report either restores the
reference or acknowledges its retirement by omitting it again; one interleaved
scan cannot permanently prune a still-live cross-PG cleanup route.

Each complete storage scan carries a process-incarnation-scoped, monotonically
increasing generation. The control plane advances retirement only for a newer
generation and ignores the route-reference portion of a retransmitted generation.
Losing a heartbeat response and retrying the same authenticated report therefore
cannot count one storage scan as two omissions. A new node incarnation resets the
accepted generation before its first report; there is no cross-process monotonic
clock assumption and no older control-plane RPC encoding is accepted. Once the
reported reference set is unchanged and no reference is retiring, accepting a
newer scan generation is a durable no-op; the volatile node view may advance the
generation without appending another journal or Raft WAL entry.

Storage-node lease renewal is independent of complete heartbeat collection. The
sender retains the latest complete report and may retransmit that exact report
while the next PG scan waits for a busy PG. Complete reports and retransmissions
share one submission order, and a newly built report becomes the retained report
before dispatch, so response loss cannot allow an older report to follow a newer
one. A complete-scan response is removed from that submission section before
any local topology read, construction, validation, or publication, each of
which may wait for admitted storage frames or a topology lock.
Cached retransmission renews only the control-plane node lease and does not
read, construct, validate, or publish local topology. It calls the authority
directly and extracts only the returned lease, so a blocked local topology read
or publication cannot block subsequent lease renewal. Scan/publication and
authority-renewal health are recorded independently; successful renewal cannot
hide a permanently failing topology publisher. The complete-scan worker remains
the sole runtime-map publisher.
Retransmission cannot advance route-reference retirement; only the next complete
scan generation can do that.

A storage node reports pending-command evidence for every opened PG, including
a PG from whose current acting set it has been removed. Non-acting observations
are accepted only when they contain a pending command and retained history proves
the authenticated reporting node was that command epoch's active primary.
Ordinary stale PG state remains discarded. Recovery discovery scans this
independently authorized historical-primary evidence across all node records, so
an offline old primary can make its durable slot discoverable after an acting-set
change. Accepting that evidence fences an already Active replacement route into
Peering and reinserts the evidence after the resulting epoch transition, so the
route cannot continue serving or erase the recovery task. Replaying the same
stale evidence while the PG is already Peering restores the observation without
advancing the global epoch again. Clearing the slot removes the observation on
the next complete heartbeat.

Response construction must not introduce a new fallible storage or parsing
boundary after publication. Bucket metadata mutations return a receipt derived
from the exact applied command, including its bucket execution generation;
they do not issue a second bucket read to construct success or invalidate the
frontend cache. This includes the `Created` outcome from `CreateBucket`.
`CreateBucket::Exists` carries the pre-publication bucket snapshot needed to
apply AWS ownership and legacy-region semantics; it is not reconstructed after
a create command publishes. Object operations must parse stored lifecycle
configuration and resolve every other fallible response input before publishing
object metadata. After publication, only infallible projection of the
already-validated inputs and command outcome is permitted.
For direct PUT, exact witness-plus-primary publication returns the response
projected from `CommitDirectPutObject` immediately. It must not re-enter payload
ack registration or validation; the retained pending slot owns trailing
replica convergence and terminal cleanup.

Cross-PG dependency commands are stricter. Publishing a dependency command on
its witness and primary is irrevocable, but does not authorize publication of
the dependent PG command. The dependency finisher must require convergence on
the complete acting set. If a trailing replica cannot be confirmed within the
bounded request work, it returns the typed internal
`MetadataCommandDependencyConvergencePending` outcome, leaves the exact command
and reservation durable for recovery, and does not publish the dependent
command. This outcome is not metadata contention and must not map to
caller-visible `SlowDown`.

DeleteBucket's post-reservation exact-bucket scan is also a destructive cross-PG
barrier. It must converge each same-bucket pending command on the complete
acting set and complete terminal slot cleanup before advancing the durable
object-PG frontier. Published recovery handoff or deferred cleanup may be
retried within the shared delete budget, but neither is a completed drain.

An ambiguous witness or primary response is different from a confirmed
publication. The request retries the exact command under a short absolute
confirmation deadline. If it still cannot distinguish applied from not
applied, it returns an internal outcome-unconfirmed error, never `SlowDown`, and
retains the pending slot for recovery. A later request must drain that exact
slot before preparing another command. Ambiguous loss after confirmed primary
publication is resolved by the same exact-command checks or handed off as
published recovery; it never issues a replacement. Transient terminal cleanup
after convergence is also deferred and cannot replace the committed response
with an error.

A definitive routing or transport-admission failure after the witness returns
the internal `MetadataCommandIrrevocableConvergencePending` outcome rather than
caller-retryable contention. A definitive semantic, command-byte, checksum, or
hash-chain failure is preserved verbatim for diagnosis and fail-closed
recovery; it must not be overwritten as generic outcome uncertainty.

Irreversible uncertainty is not historical-route authority. When a normal-route
owner cannot finish either internal uncertainty outcome, it records that exact
outcome in the process-local single flight and relinquishes execution without
removing the flight. Normal waiters remain blocked behind that handoff and
cannot fall back to old-route inspection. Only the current-map recovery worker
may claim the detached flight, after matching the control-plane certificate to
the flight's immutable lineage root. Reissue may advance the durable pending
command to a later lineage tip, but does not replace that certified root. A
replacement whose mutation response and bounded exact-slot reconciliation are
both inconclusive is recorded as outcome uncertainty for the already-bound
lineage tip; the transport error cannot discard the C1-to-C2 flight. Fatal
authenticated-response, protocol, and integrity failures remain the foreground
error, while the independently retained flight still protects an ambiguously
committed C2 for authorized recovery. A cleanup derivative additionally retains
its exact abandoned predecessor and the root command's abandoned disposition.
If the ambiguous replacement did not commit and durable inspection still finds
C1, rebinding that exact speculative C1-to-C2 derivative is idempotent; no
different derivative is admitted. Authorized takeover supplies the predecessor
to every storage mutation, and successful derivative convergence resolves
waiters as C1 abandoned rather than C2 applied. A delayed storage-node
certificate or other retryable recovery-route rejection
returns the worker error while restoring the detached handoff; it does not wake
foreground waiters or replace the retained internal uncertainty with
`SlowDown`. Terminal recovery records its exact applied or abandoned outcome
before releasing the flight.

A current primary reporting an exact pending command from the current epoch
does not by itself invalidate an otherwise healthy Active route. The authority
keeps the PG Active, does not advance the global epoch, and publishes the exact
pending identity and reporting primary as recovery authorization in the
current runtime map. The current-map recovery worker may use that certificate
with the current route fence to converge the command; ordinary request paths
remain serialized behind the PG pending slot. A later primary scan that no
longer observes the slot removes the certificate without changing the epoch.
Pending evidence from an older route is different: it requires the retained
Active historical-primary route and keeps the current PG in Peering until
historical recovery resolves it.

The handoff is local ownership bookkeeping, not a new storage mutation. It must
therefore be recorded even when the request work deadline has just expired.
Expiry still prohibits additional normal-route mutation; it does not authorize
dropping an already irreversible flight or its C1-to-C2 lineage.

Standalone one-replica mode has no second failure domain and therefore has no
off-primary witness. Its configured lack of redundancy is explicit; the same
exact-command retry and pending-slot rules still prevent replacement after an
ambiguous primary dispatch.

This contract prevents Argmin from returning an explicitly retryable S3 error
after publishing a version that a retry could duplicate. A process or network
failure can still prevent delivery of the final HTTP response; as with AWS S3,
that transport-level outcome is inherently ambiguous to the client. Argmin
must not manufacture the same ambiguity as an application-level `SlowDown`.

Authorization is decided before command admission. A policy change racing an
already admitted command does not revoke that command during irrevocable
convergence; either admission loses the race and the primary does not apply,
or the exact admitted command converges and returns its committed result.

Any process routed to the same PG primary must observe the same unresolved
slot. Retrying through a different coordinator must therefore converge the same
command instead of allocating a different command.

On each replica, applying the metadata mutation and recording or advancing the
command-log state is one SQLite transaction. A replica must not commit
materialized metadata without the matching command-log record, and must not
commit a command-log record that claims a mutation occurred without the
matching materialized metadata mutation. Failure injection must cover rollback
of both directions.

On restart, acting-set replicas normally must agree on the accepted command
prefix and materialized-state digest before the PG is considered clean. The
only accepted in-flight exception is a primary-owned pending slot for the next
log index where every advanced replica has accepted exactly that command,
chained from the primary's current log hash, all unadvanced replicas match the
primary's current replica state, and all advanced replicas agree on the full
post-command replica state including the materialized-state digest. Any other
prefix or digest disagreement remains a fail-closed divergence. When the
in-flight exception is accepted, open-time recovery must converge that command
and remove the primary pending slot before returning the cluster map; read-only
paths must never serve stale primary materialized rows while relying on the
advanced replicas' command log state.

Normal and recovery convergence inspection treats a missing log entry or a
`MetadataCommandLogConflict` from any acting-set member as “not applied on all
acting nodes.” It must not accept that command as converged or classify a
same-index conflict as proof of application; the owning finish/recovery path
must retain the pending command and perform the exact-command checks described
below.

## Publisher Classification

Command safety is classified at the publisher path, not just by command kind.
The risk depends on which metadata snapshot, authorization result, or request
precondition was used before the pending slot was installed.

For frontend-admitted publishers, checking admission before calling the
installer is insufficient: lock waits and RPC transport can cross the captured
deadline while the raw same-epoch route is renewed. Their immutable
`AdmittedRouteEffectFence`, including the admission's conservatively bounded
deadline, is therefore carried to the effect boundary. Operation RPCs serialize
only the portable wall-clock upper bound; each receiving process rebinds it to
its own monotonic clock. Embedded and RPC nodes revalidate clock health and
their local effective deadline immediately before the durable pending-slot
insert.

Publisher classes:

- `SnapshotSensitive`: the publisher must rebuild the command from a fresh
  snapshot after pending-slot contention. Draining a competing slot and
  installing the old command is not safe.
- `ApplyValidated`: command apply fully validates the state this publisher
  depends on. The same command may be retried after draining a competing slot,
  subject to the normal reissue/hash-chain rules.
- `AllocatorCleanup`: the command is cleanup or allocator state with a known
  external owner. It must not steal another request's resources when a pending
  slot is drained.
- `MatchingOutcomeRetry`: the publisher is snapshot-sensitive for unrelated
  contention, but an equivalent pending command is also the result the caller
  is waiting for. These publishers must not use a generic drain-and-retry path
  for a matching contender, because draining the command can delete the
  request state needed to reconstruct the response. They need an explicit
  matching predicate, an outcome extractor from the command-owned row image,
  and a same-command install-race regression.
- `TerminalSessionRetry`: the publisher is snapshot-sensitive and has a
  terminal command that deletes the stream/upload session it is finalizing or
  aborting. An equivalent contender must remain visible to the publisher's
  top-of-loop matching branch; generic drain can delete the session before the
  retry can validate or finish the matching command.

Command-carried bucket reservations have one exhaustive
`MetadataCommandReservationDisposition`, shared by foreground terminal cleanup,
local-open convergence, authorization-proof selection and pending route retention:

- Ordinary command proofs are released after successful application. This includes
  UploadPart's short-lived creation proof and its separate finalization proof.
- Streamed PUT creation retains its proof for the live session. Successful
  creation is not permission to release it; later commit or abort owns release.
- Stream abort's optional creation proof is cleanup evidence, not the abort's
  own mutation proof. Applied abort releases it; abandoning an unstarted abort
  must preserve the live session's proof.
- A command with no carried proof does not establish that an external resource
  was released. Direct PUT's generation-reservation command currently carries
  no bucket proof; its classification destructures all fields so adding the
  planned owner proof forces a release-policy review.

Route retention includes both retained and releasable proofs until pending work
settles. Do not use that dependency iterator as a generic release list. This is
current command policy, not the future durable owner-obligation/release receipt.

Direct streamed `PutObject` session liveness is represented by the
stream-create bucket-write reservation proof, not by the stream row age. A live
frontend refreshes that proof while the request can still append or finalize.
`DeleteBucket` must treat a direct `PutObject` stream row with a validating
proof as live bucket contents and return the normal non-empty result; if the
proof is missing, mismatched, or expired, the stream is abandoned staging and
may be aborted during DeleteBucket's synchronous drain. The background stream
session sweeper uses the same proof-validation rule so abandoned sessions are
eventually cleaned even without a DeleteBucket request.

Current production pending-command publishers:

| Publisher path | Command kind | Class | Required contention shape |
| --- | --- | --- | --- |
| `create_bucket_with_config_and_load_info_with_route_validation` | `CreateBucket` | `ApplyValidated` | Revalidate the admitted bucket route before command construction and carry its immutable effect fence to pending-slot insertion; drain competing PG slot during pending-slot checks and command-id allocation, while exact-row apply handles idempotence/conflict. |
| `begin_bucket_delete_if_current_with_route_validation` | `MarkBucketDeleting` | `SnapshotSensitive` | Revalidate the admitted bucket route before the durable write drain and pending-slot insertion, then rebuild from current bucket/delete preconditions after contention. |
| `delete_bucket_from_acting_set` | `DeleteFinalizedBucket` | `SnapshotSensitive` | Rebuild from the current deleting-bucket generations after unrelated contention; an equivalent pending finalization may be finished only after its exact bucket identity is matched. |
| `put_bucket_versioning_with_route_validation` | `PutBucketVersioning` | `SnapshotSensitive` | Drain competing PG slot during pending-slot checks and command-id allocation; rebuild bucket post-image after contention. |
| `put_bucket_acl_with_route_validation` | `PutBucketAcl` | `SnapshotSensitive` | Drain competing PG slot during pending-slot checks and command-id allocation; rebuild bucket post-image after contention. |
| `put_bucket_property_command_with_route_validation` | `PutBucketProperty` | `SnapshotSensitive` | Drain competing PG slot during pending-slot checks and command-id allocation; rebuild bucket post-image after contention. |
| `put_bucket_subresource_command_with_route_validation` | `PutBucketSubresource` | `SnapshotSensitive` | Drain competing PG slot during pending-slot checks and command-id allocation; rebuild bucket post-image after contention. |
| `reserve_put_object_generation_with_route_validation` | `ReserveObjectGeneration` | `AllocatorCleanup` | Revalidate admitted PutObject authority before each allocation attempt and carry its immutable effect fence to pending-slot insertion. Drain competing PG slot before and during command-id allocation, reread the allocator before final command-id allocation, then allocate/reuse through the reservation owner. If the generation becomes stale before publish, exact `ObjectGenerationReservationConflict` is retried without releasing a non-matching reservation. |
| `reserve_next_object_version` | `ReserveObjectVersion` | `AllocatorCleanup` | Drain competing PG slot before and during command-id allocation, then allocate/reuse through the version reservation owner. |
| `release_object_generation_reservation_command_required` | `ReleaseObjectGeneration` | `AllocatorCleanup` | Required cleanup must retry through slot contention, including command-id contention, until terminal or fail without losing the cleanup intent. |
| `release_object_generation_reservation` | `ReleaseObjectGeneration` | `AllocatorCleanup` | Release an already-owned standalone Direct PUT reservation; stream-owned generations require their atomic terminal/abort command. Drain competing PG slots before and during command-id allocation and retry. |
| `commit_direct_put_object_from_payload_shards_with_route_validation` | `CommitDirectPutObject` | `SnapshotSensitive` | Revalidate the admitted PutObject route while rebuilding the commit from fresh object preconditions and stale-payload snapshots after pending-install contention or pre-publish command-id contention. Carry the immutable effect fence to version reservation and final pending-slot insertion. The commit command carries a durable bucket-write reservation proof and terminal convergence releases it before removing the pending slot. |
| `create_put_object_stream_session_record_under_reservation` | `CreateStreamUpload` | `SnapshotSensitive` | Low-level PutObject stream-create publisher. The public wrapper first holds a durable bucket write reservation; this internal publisher rebuilds session command and reservation cleanup from fresh object state after contention. |
| `commit_stream_segment_append` | `AppendStreamSegment` | `ApplyValidated` | The actual object-PG primary builds the command under its PG lock, validating the session/target, segment witness, live allocation lifetime and captured effect fence. Build-time and pending-install contention return through the existing one-contender retry loop without deleting referenced staged payload; replica apply independently validates the same durable session/segment invariants. |
| `abort_stream_upload_session` | `AbortStreamUpload` | `TerminalSessionRetry` | Rebuild staged-segment snapshot after unrelated pending-slot or command-id contention. If an equivalent terminal command wins the pending slot, restart without draining so the matching/session-completion branch can finish it. |
| `put_object_metadata_if_with_route_validation` | `PutObjectMetadata` | `SnapshotSensitive` | Revalidate request-scoped route authority and rerun request action/preconditions after contention. The command carries a durable bucket-write reservation proof and terminal convergence releases it before removing the pending slot. |
| `delete_specific_object_version_if_with_route_validation` | `DeleteObjectVersion` | `SnapshotSensitive` | Revalidate request-scoped route authority and rerun delete preconditions after contention. The command carries a durable bucket-write reservation proof and terminal convergence releases it before removing the pending slot. |
| `delete_current_object_if_with_route_validation` | `DeleteObjectVersion` | `SnapshotSensitive` | Revalidate request-scoped route authority and rerun current-object selection after contention. The command carries a durable bucket-write reservation proof and terminal convergence releases it before removing the pending slot. |
| `insert_current_delete_marker_if_with_route_validation` | `InsertDeleteMarker` | `SnapshotSensitive` | Revalidate request-scoped route authority and rerun current-object/versioning selection after contention. A matching pending marker is finished as the request's outcome only if it applies; if its snapshot becomes stale and it is abandoned, the request discards the computed result and re-evaluates its condition against a fresh object snapshot. The version-ID reservation and marker command are both fenced at their durable insertion boundaries. Each command carries a durable bucket-write reservation proof where applicable, and terminal convergence releases it before removing the pending slot. |
| `expire_current_object_if_due_raw` | `DeleteObjectVersion`/`InsertDeleteMarker` | `SnapshotSensitive` | Rerun lifecycle selector and object-lock checks after contention. The command carries a durable bucket-write reservation proof and terminal convergence releases it before removing the pending slot. |
| `delete_noncurrent_live_versions_if_due_raw` | `DeleteObjectVersion` | `SnapshotSensitive` | Rerun lifecycle selector and object-lock checks after contention. Each delete command carries a durable bucket-write reservation proof and terminal convergence releases it before removing the pending slot. |
| `delete_expired_delete_marker_if_due_raw` | `DeleteObjectVersion` | `SnapshotSensitive` | Rerun expired-marker selector after contention. The command carries a durable bucket-write reservation proof and terminal convergence releases it before removing the pending slot. |
| `reclaim_object_payload_if_unleased` | `DeleteObjectPayloadReclaim` | `SnapshotSensitive` | Recheck lease/fence and reclaim state after contention, including command-id contention after payload cleanup has started; cleanup must preserve retryability. |
| `create_put_object_stream_session_with_route_validation` | `CreateStreamUpload` | `SnapshotSensitive` | Rerun authorization/object snapshot after contention; enforce the request admission's immutable effect fence at reservation and pending-command insertion; frontend streams also persist the admitted route deadline for durable cleanup handoff. |
| `finalize_put_object_stream_with_route_validation` | `CommitDirectPutObject` | `TerminalSessionRetry` | Rebuild commit from current object preconditions and stream-session snapshot after unrelated contention. If an equivalent terminal command wins the pending slot, restart without draining so the matching branch can finish it while the session state is still coherent. Production stream finalization carries the request admission's immutable effect fence and a durable bucket-write reservation proof; terminal convergence releases the proof before removing the pending slot. |
| `create_multipart_upload_inner_with_route_validation` | `CreateMultipartUpload` | `SnapshotSensitive` | Revalidate the request-scoped multipart object route and rerun authorization/object snapshot after contention. The reservation acquisition and pending-command installation both enforce the admitted effect fence. The command carries a durable bucket-write reservation proof and terminal convergence releases it before removing the pending slot. |
| `create_upload_part_stream_session_with_route_validation` | `CreateStreamUpload` | `SnapshotSensitive` | Revalidate the admitted multipart object route and MPU/session target after contention. UploadPart and UploadPartCopy carry the immutable request effect fence through reservation acquisition and pending-command insertion, persist the admitted cleanup deadline, and release the durable bucket-write proof only after convergence. |
| `establish_multipart_completion_barrier_inner` | `AdvanceMultipartCompletionBarrier` | `AllocatorCleanup` | Drain competing bucket-PG slots, validate the current completion bucket-write proof, allocate and replicate a fresh monotonic scalar, and only then publish the object-PG completion. A pre-existing or contender barrier is not proof for the current reservation because the command carries no reservation identity; return to the owner loop and build a fresh barrier after contention. No terminal upload row is created. |
| `complete_multipart_upload_commit_serialized_with_route_validation` | `CommitMultipartObject` | `MatchingOutcomeRetry` | Revalidate the admitted multipart object route while rebuilding completion parts, cleanup snapshot, stale payload, and bucket-PG barrier after unrelated contention. The completion reservation, version reservation, barrier insertion, and final object-PG insertion all enforce the request admission's immutable effect fence. If the contender is the same completion request, finish that exact command through the matching-pending branch and return its computed outcome. The command carries a durable bucket-write reservation proof and terminal convergence releases it before removing the pending slot. |
| `finalize_upload_part_stream_with_route_validation` | `CommitStreamPart` | `TerminalSessionRetry` | Revalidate the admitted multipart object route while rebuilding the stream session, MPU row, staged segments, and displaced part refs after unrelated contention. Carry the immutable request effect fence through the finalization reservation and pending-command insertion. If an equivalent terminal command wins the pending slot, restart without draining so the matching branch can finish it. The command carries a durable bucket-write reservation proof and terminal convergence releases it before removing the pending slot. |
| `abort_multipart_upload_locked` | `AbortMultipartUpload` | `TerminalSessionRetry` | Rebuild upload, part, active stream session, staged segment, and cleanup snapshots after unrelated contention. If an equivalent abort wins the pending slot, restart without draining so the matching branch returns the successful abort outcome. |
| `abort_authorized_multipart_upload_locked` | `AbortMultipartUpload` | `TerminalSessionRetry` | Rebuild authorized upload cleanup snapshot after unrelated contention and compare the current upload row to the authorized row before install. The foreground S3 path acquires its reservation and installs its pending command through the request admission's immutable effect fence. If an equivalent abort wins the pending slot, restart without draining so the matching branch returns the successful abort outcome. |

Adding a production call site that creates or installs a pending metadata
command requires adding it to the authoritative Rust registry and updating
this table. Production publisher entry points cannot directly use
`try_set_pending_metadata_command_for_bucket`,
`try_install_pending_metadata_command_for_bucket`,
`try_install_object_pg_built_pending_command`, and
`set_pending_metadata_command_for_bucket`; the boundary checker rejects any
such call outside the compiler-classified typed installer implementations.
Snapshot-sensitive publishers use
`install_snapshot_sensitive_metadata_command_or_drain` so slot contention
drains exactly the observed winner and returns to the caller's fresh-snapshot
loop. The typed installer must not drain until the slot is empty: each owner
iteration rechecks its request work budget and admitted route authority before
handling another contender.
Allocator/cleanup fresh-command installation delegates to
`install_allocator_cleanup_built_metadata_command`. Its fallible builder runs
only after the pending-slot check under frontend metadata-command PG
serialization and a check of the caller's existing work budget. It can return
a complete primary-built command rather than requiring the frontend to allocate
the ID separately. A construction-time log conflict follows the same
one-observed-contender drain and recovery-handoff outcomes as installation-time
contention; other construction errors propagate without installing a slot.
The returned envelope must match the PG and route epoch, and insertion retains
the supplied immutable effect fence. This frontend serialization is not a
substitute for primary-side transactional validation or deadline checks in a
remote builder. Direct PUT's scoped generation route now supplies a primary-built
reservation command: under the primary PG lock it checks reservation absence,
revalidates the expected generation, resolves the selected data placement and
checks the effect fence immediately before selecting the command ID. A stale
candidate returns to the outer loop without installation. Unix/TLS intersects the
delegated deadline with the server's captured route fence and validates the
returned PG, epoch, bucket, key, reservation ID and generation. Version/release
builders retain the fresh-ID adapter. Neither builder path publishes a pending
slot; that ownership transfer remains the installer's responsibility.
Stream append uses `install_apply_validated_built_metadata_command` with the
same fallible built-command primitive. The frontend no longer constructs
`AppendStreamSegment` from a separately selected ID. The actual primary performs
read-only preflight and ID selection under its PG lock; Unix/TLS carries the
absolute portable deadline, intersects the server's captured fence, and validates
the returned subject, target, entire segment, PG and epoch. Construction adds no
durable write or fsync. It does require a remote builder RPC; no latency saving
is claimed. The pending-slot insertion still rechecks the immutable effect fence,
and replicas retain apply-time validation rather than trusting preflight.
Terminal-session publishers use
`install_terminal_session_retry_metadata_command`, whose exhaustive result
distinguishes installation, a matching visible contender, an unrelated visible
contender, and contention without a visible slot. The helper never drains a
contender: the owner loop must preserve a matching terminal command and may
drain unrelated work only after it has rerun its matching check.
Matching-outcome publishers use
`install_matching_outcome_retry_metadata_command`, whose exhaustive result
also preserves a matching visible command instead of draining it. The command
envelope carries the response row and reservation ownership evidence needed by
the retrying caller. Unrelated or no-longer-visible contention returns to the
owner loop, which reruns its matching check before any generic drain. Every
matching-outcome publisher must be documented above and covered by an
install-race regression where the equivalent command wins after command
construction.
If a PG-wide pending slot appears after the publisher has taken its snapshot
but before it allocates the command id, `MetadataCommandLogConflict` is the
same pre-publish contention class: the publisher must drain the winner and
restart from a fresh snapshot, not surface the conflict to the request.

The registry generates one sealed publisher token per row. Typed installers
accept only tokens whose registry class matches their retry/convergence path;
changing a migrated publisher's class therefore breaks its production call
site at compile time. Typed calls no longer participate in the temporary shell
helper-count inventory, although the boundary check still requires their
authoritative registry marker and rejects every direct call or function-item
reference to the private-field token constructor. Class-specific seals are
implemented only by the corresponding registry macro arms, so one publisher
class cannot acquire another class's authority inside the crate.

The object-PG snapshot-sensitive installer returns exhaustive, must-use
`SnapshotSensitiveInstallOutcome`. Allocator/cleanup publication uses separate
exhaustive, must-use fresh-command and prebuilt-command outcomes. Those paths
distinguish installed work from a drained pending-slot contender and handled
log-index contention, so retry cannot be mistaken for successful allocation or
cleanup. Terminal-session publication returns the exhaustive, must-use
`TerminalSessionRetryInstallOutcome`; matching command evidence is carried in
that type and is never generically drained inside the typed installer.
Matching-outcome publication provides the corresponding exhaustive, must-use
`MatchingOutcomeRetryInstallOutcome`, retaining the exact command-owned result
until the caller can converge it.
Bucket-control snapshot-sensitive publication uses the same exhaustive
`SnapshotSensitiveInstallOutcome` at its distinct fenced control-slot
boundary. Versioning, ACL, bucket-property, and subresource publishers must
therefore handle a drained contender by restarting from their current bucket
snapshot rather than treating the attempted install as successful.
Bucket-delete begin and finalization use the same exhaustive outcome at the
bucket-PG slot boundary. Fresh mark/finalize commands restart after a drained
contender, while finalization retains its separate exact-command branch so a
matching already-pending delete can converge without being rebuilt or drained.
Apply-validated publication likewise has separate exhaustive, must-use
fresh-command and prebuilt bucket-PG outcomes. Stream append preserves the
distinction between a drained visible contender and a handled log-index
conflict because each path has its own retry-budget diagnostic and payload
reference-check boundary; CreateBucket must explicitly restart when its
prebuilt command loses the pending-slot race.
Publisher paths which still call lower-level installers remain inventoried here
and in
`scripts/check-storage-cluster-boundaries` until their Phase 4 typed API is
introduced.

## Multipart Command-Stream Invariants

Phase 9.3 owns multipart upload lifecycle serialization. Multipart commands are
still PG-local commands: the bucket PG owns the fixed-size completion barrier,
while the object PG owns upload, part, stream session, object publication, and
cleanup rows. Multi-PG multipart flows must derive later commands only from
terminal earlier commands; they must not leave two PGs with ambiguous pending
slots where either PG cannot determine whether the request should converge or
restart.

Command-owned multipart metadata includes:

- `multipart_uploads`
- `multipart_parts`
- `multipart_part_segments`
- `stream_uploads`
- `stream_upload_segments`
- object-row multipart completion upload identity and request fingerprint
- `buckets.multipart_completion_barrier_sequence`

For one upload ID, the command stream must make at most one active lifecycle
outcome visible: an in-progress upload row or completion replay attached to its
published object version. Abort leaves neither. Once a complete or abort command
is terminal, no active UploadPart stream session or staged stream segment for
that upload may remain valid. Cleanup of active
UploadPart stream sessions, staged stream segments, omitted parts, displaced
part segments, and UploadPartCopy staged copied segments must be carried by the
terminal command or by explicit retry/scavenger records. Later cleanup must not
infer those refs from current rows after the terminal command has already
published.

UploadPartCopy is part of the same surface as streamed UploadPart. It creates a
destination UploadPart stream session, appends copied source segments, finalizes
the destination part, and aborts the session if source read/copy work fails.
Phase 9.3 tests must therefore cover UploadPartCopy races with destination MPU
abort and complete, plus cleanup of copied staged segments when the destination
upload becomes terminal before finalize.

The old process-local multipart-completion lock is not part of the completion
path. Completed-upload ordering and object publication correctness must come
from durable PG-primary pending slots, command apply validation, and
fresh-snapshot restart after contention. Remaining bucket locks are not
command-stream authorities and must not be used as production correctness
boundaries.

Multipart publisher rules:

- `create_upload_part_stream_session_with_route_validation` must reload the
  in-progress MPU row after object-PG slot contention and compare it with the
  request-entry authorized upload record. This uses the request-entry
  authorization snapshot/capability; it does not mean storage reauthorizes
  against current bucket policy.
- `finalize_upload_part_stream_with_route_validation` must reload the stream
  session, MPU row, existing part row, staged segments, and displaced part
  refs after contention. Its `CommitStreamPart` command carries the
  bucket-write reservation proof; live retry and open-time convergence must
  validate and release that proof before clearing the terminal pending slot.
- `abort_multipart_upload_locked` and
  `abort_authorized_multipart_upload_locked` must rebuild upload, part, active
  stream session, staged segment, and cleanup snapshots after contention.
  Matching pending abort contenders are inspected before generic drain because
  draining them can delete the upload row needed to report a successful abort;
  these paths release their unused bucket-write proof before finishing the
  matching abort command.
  Authorized abort must compare the current upload row with the already
  authorized row before publishing; if the row changed, storage returns the
  normal missing/non-abortable outcome instead of applying a stale
  authorization snapshot. Foreground authorized abort also binds reservation
  acquisition and pending-slot installation to its request admission; raw
  unbounded authority remains test-only.
- `complete_multipart_upload_commit_serialized_with_route_validation` must
  reserve completed-MPU order through a fully converged bucket-PG command before
  constructing the object-PG commit, then reload the object-PG completion
  snapshot after that dependency converges. A bucket-PG recovery handoff is not
  sufficient authorization to publish the object-PG command. If another
  object-PG command wins the pending slot before the
  completion command id is allocated, completion drains the winner and restarts
  from a fresh object-PG snapshot so stale-payload and cleanup refs are rebuilt.
  If an equivalent completion command wins the pending slot after the snapshot
  and command construction, completion must not drain it generically; the retry
  loop must observe the matching pending command, finish it, and return the
  command-derived success outcome.
- A retry of a duplicate UploadPart or finalize request is accepted only when
  the command-owned row image and cleanup refs match exactly, except for
  explicitly documented idempotence fields such as normalized timestamps.

## Finish And Convergence Paths

Publishing a pending command is only the first boundary. A request can also
fail while finishing a pending command after one or more acting-set replicas
have already accepted, mutated, or logged it. Those finish errors must be
classified separately from pending-slot install contention.

The general rule is fail closed unless the caller has a positive proof that
the conflict belongs to the exact same command and hash-chain state. A broad
`MetadataCommandLogConflict` match is not enough: the same PG, epoch, and log
index can also describe a divergent command-log row. A partial exact-command
retry proof must be established by replicas that applied before the failing
replica; later replicas may only confirm that already-established command
chain. If the first failing replica already has the exact command row, retry is
allowed only after validating that row's bytes, checksum, `previous_log_hash`,
and `log_hash` against the primary prefix; a same-index conflict without that
hash-chain proof is still divergent state and must fail closed.

Object-PG pending slots have two separate finish APIs:

- Exact request outcome: the caller has already matched the PG-slot command to
  the request whose result it will return. It must pass an
  `ExactPendingObjectMetadataCommand` proof token and handle `Applied`,
  `Abandoned`, and retryable partial-exact outcomes explicitly.
- Generic drain: the caller only needs to make PG-slot progress before
  restarting from a fresh snapshot. It must not report the drained command as
  the current request's success, and it must not delete extra request state
  outside the command finisher.

The old bucket-named object finisher shape is banned because it hid the fact
that the slot is PG-scoped. Draining a command for another bucket, key, upload,
or generation is normal contention progress; treating it as this request's
outcome is only valid after an exact matching predicate has succeeded.

| Finish caller/path | Command scope | Finish classification | Notes |
| --- | --- | --- | --- |
| `create_bucket_with_config_and_load_info_with_route_validation` | bucket PG | Abort only before publication-start; exact confirmation or recovery handoff afterward. | A confirmed primary publication is returned as success while trailing convergence retains the exact slot. |
| `put_bucket_versioning_with_route_validation` | bucket PG | Abort only before publication-start; exact confirmation or recovery handoff afterward. | A fresh request snapshot is allowed only before irrevocable convergence; success carries a command-derived mutation receipt. |
| `put_bucket_acl_with_route_validation` | bucket PG | Abort only before publication-start; exact confirmation or recovery handoff afterward. | A fresh request snapshot is allowed only before irrevocable convergence; success carries a command-derived mutation receipt. |
| `put_bucket_property_command_with_route_validation` | bucket PG | Abort only before publication-start; exact confirmation or recovery handoff afterward. | A fresh request snapshot is allowed only before irrevocable convergence; success carries a command-derived mutation receipt. |
| `put_bucket_subresource_command_with_route_validation` | bucket PG | Abort only before publication-start; exact confirmation or recovery handoff afterward. | A fresh request snapshot is allowed only before irrevocable convergence; success carries a command-derived mutation receipt. |
| `begin_bucket_delete_if_current_with_route_validation` | bucket PG | After publication-start, partial exact-command conflicts are retryable only after validating exact command bytes plus matching `previous_log_hash` and `log_hash`; divergent same-index rows fail closed. | Once the primary publishes `Deleting`, a recoverable trailing error returns the committed outcome and retains the slot for recovery. |
| `establish_multipart_completion_barrier` | bucket PG | Abort only before publication-start; after publication-start, retain the exact command and require complete acting-set convergence. | A published but unconverged barrier returns an internal dependency-pending outcome and must not publish the object-PG command. The returned idempotence sequence comes only from the exact fully converged barrier. |
| `drain_pending_metadata_command_pg_slot` and `drain_pending_multipart_completion_barrier_command` | bucket PG drain | Fail closed on unsafe finish conflicts. | These are generic drain helpers; they must not hide divergent command-log state from the caller. |
| `finish_pending_command_for_multipart_completion_barrier` | bucket/object PG drain | Requires complete acting-set convergence for both bucket and object commands, using the barrier caller's existing absolute work budget. | Multi-PG MPU completion must not hand a published object command to recovery and then publish its dependent barrier; bounded trailing failure remains dependency-pending. |

Pre-publish conflict handling belongs to the compiler-classified publisher.
Every registered publisher token implements exactly one sealed class trait,
and even its generic contender-drain paths require that token. A pre-publish
conflict branch drains at most one contender before returning to its
route-authority, work-budget, and fresh-snapshot checks. Preflight paths that
must inspect applied commands for idempotence retain a separate token-gated
collecting drain with one shared finite work budget. Terminal and
matching-outcome classes preserve equivalent pending commands through their
exhaustive typed outcomes instead of draining them.

Storage-owned inspection and recovery do not borrow a publisher token. They
must instead hold an opaque, non-copy recovery-drain authority which borrows
one request work budget across every contender. The primitive drain itself
accepts only a typed per-invocation authority derived from either that recovery
authority or a registered publisher token; it has no optional raw-budget or
default-budget path. Both authority states have fields private to a child
module, preventing same-module struct-literal construction. The boundary
scanner inventories every live recovery-authority constructor, conversion,
primitive call, recovery wrapper, recovery execution-route construction, and
recovery-only bucket finisher (including function-item references), so an
unmarked publisher cannot mint or invoke recovery authority without changing
the audited inventory. The primitive converts its joined leader guard into an
opaque leader capability. Every lower historical apply, abandonment, reissue,
and pending-slot removal boundary requires a proof borrowed from that live
capability, so neither a direct lower call nor a recovery execution-route
struct literal can escape the joined guard's lifetime. The proof is also bound
to an immutable certified root `(PG, log index, checksum)` and a current
mutation subject. For an unreissued command they are identical; an authorized
handoff retains C1 as the root while joining C2 as the subject. Every lower
boundary checks the applicable subject before storage access. Reissue returns a
newly derived proof bound to the replacement command only after validating a
same-epoch, later-index chain with either identical payload or the one explicit
abandoned-command cleanup derivative. Thus a leader for one command cannot be
redirected to another command, while legitimate recovery follow-ups remain
explicitly certified. A cleanup proof also retains the exact abandoned-command
predecessor: lower mutation and subsequent same-payload reissue both require
that context, and omission is rejected rather than silently broadening the
proof. The scanner inventories those lower calls and
struct-literal syntax as defense in depth. Production exposes no raw
all-command drain: the remaining unclassified convenience drain is test-only,
and the scanner rejects any production reference to it, including from an
unmarked wrapper.

Finish/convergence handling is a separate storage-engine boundary. A caller
may report a pending command as its request's outcome only through an
`ExactPendingObjectMetadataCommand` created after matching that exact request.
After any replica may have accepted a command, convergence still requires exact
command bytes and hash-chain proof; divergent same-index state fails closed.
These compiler-visible publisher and exact-command boundaries replace the old
shell inventory of function-name occurrence counts.

## Object Version Reservations

Object version IDs are allocated by the `ReserveObjectVersion` metadata
command before some terminal object commands are installed. Once that
reservation command has applied, losing the later terminal command's pending
slot race may leave an unused version ID if the fresh retry no longer passes
request preconditions. That gap is allowed: object version IDs are opaque
allocator outputs, and the reserved value is not published as an object version
unless the terminal object command applies.

Tests should distinguish this from precondition failures that happen before
reservation. A request rejected before `ReserveObjectVersion` must not advance
`object_version_counters`; a request that has already durably reserved a
version may advance the counter even if later pending-slot contention causes a
fresh retry to fail.

## Recovery

Before accepting new work on a PG, recovery must inspect durable command-stream
state:

- if there is a pending command slot that has not been accepted, mutated, or
  logged by any acting-set replica, retry command application or durably
  abandon it according to the command's normal rules
- if any acting-set replica accepted, mutated, or logged the command, recovery
  must converge that same command to a terminal durable record; it must not
  abandon the command and issue a later replacement
- recovery convergence must use the same witness-then-primary apply ordering as
  normal command fanout. The off-primary row proves admission and the primary
  row proves serving publication, but neither is sufficient reason to clean the
  primary pending slot until all required replicas have either converged or the
  PG has failed closed for repair.
- open-time command convergence only proves metadata convergence. Any
  post-commit best-effort physical payload cleanup that would normally run
  after the command commits can still require the later scavenger path, just as
  it would after a crash between metadata commit and cleanup.
- if a terminal log record exists for the slot, finish slot cleanup
- if a replica has a contiguous abandoned log tail but no pending slot,
  advance the replica index/hash across that abandoned tail while preserving
  the previous materialized-state digest; an abandoned tail must not bless any
  bucket/object rows that the abandoned command should not have changed
- an unadvanced applied log tail without matching materialized state is not
  recoverable and must fail closed
- if the materialized metadata digest disagrees with the accepted log prefix,
  fail closed and require repair
- if command-log prefix state is incomplete, do not allocate a later command

A process-local crash must not lose the fact that a command partially applied.
A different process must be able to finish the same slot.

The abandon boundary is therefore exactly the zero-replica-apply boundary. A
command may be abandoned only while no acting-set replica has accepted the
mutation, changed materialized state, or recorded the command/tombstone. Once
that boundary is crossed, retries and recovery must finish the original
command, including any matching terminal tombstone, before new work can start on
the PG.

## Multi-PG Requests

Phase 9.2 defines PG-local command streams. It does not provide cross-PG
transactions.

Some request flows already sequence commands across more than one PG. For
example, multipart completion establishes its bucket-PG barrier and then commits
object metadata on the object PG. These flows must use a deterministic PG order
and explicit retry cleanup. They must not leave
ambiguous pending slots on multiple PGs where either PG cannot decide whether
to converge or abandon its own command independently.

The rule for these flows is:

- each PG command slot is independently recoverable using the PG-local recovery
  rules above
- later PG commands must either be derived from already-terminal earlier PG
  commands, or have explicit cleanup when a later step cannot proceed
- no process-local lock may be used to make the multi-PG sequence appear
  atomic

Cross-PG atomicity, if needed later, is a separate design from Phase 9.2.
Targeted multipart race coverage for these multi-PG flows belongs to Phase
9.3, after Phase 9.2 has provided the PG-local command-slot primitive.

## Stream Segment IDs

Stream segment VID allocation is part of the metadata command stream model.
It must not be owned by `LocalClusterRuntimeState` or any other process-local
map.

The implemented rule is:

- the stream session row has a durable `next_segment_vid` allocator on the PG
  primary, updated by SQLite before payload shards are written
- the append command carries the allocated segment VID, and command apply
  advances each replica's allocator floor to at least the committed VID plus
  one
- allocated but uncommitted VIDs are allowed gaps; they are allocator state, not
  object metadata or object data in the command log
- terminal stream commands delete or make irrelevant the stream session row, so
  no process-local allocator cleanup is required

The allocator column is not part of the canonical metadata command digest. The
command-owned state is the append segment row and the payload CRC/hash carried
by that row; the allocator is durable PG-primary coordination state used to
avoid cross-process shard-key reuse before the append command exists.

This split is intentionally a little different from ordinary command-owned
metadata. `stream_upload_segments.segment_vid` is semantic metadata: it is the
payload shard identity for a staged segment, so the exact value is encoded in
`AppendStreamSegment` and replicated through command apply. By contrast,
`stream_uploads.next_segment_vid` is only an allocator floor. It may be ahead
on the primary while append commands are still in flight, and replicas may
advance to the same or a lower floor until those commands apply.

Terminal MPU commands do not encode full `StreamUploadRecord` rows. They use a
terminal cleanup record containing the stream session identity, target, state,
creation time, and encryption state, but not `next_segment_vid`. That keeps
the command-owned cleanup state separate from the durable runtime allocator
floor, and avoids equality checks that have to remember to ignore allocator
progress.

`CreateStreamUpload` also avoids carrying a broad `StreamUploadRecord`. Its
payload contains the command-owned session projection, an explicit initial
`next_segment_vid` floor and the selected `payload_generation`. Retry matching
must compare all three: session
identity/state/encryption proves the command-owned row, while the initial floor
proves the allocator started from the expected value, and the exact durable
generation reservation proves payload ownership. Later allocator progress
belongs to append allocation and append commands, not to generic stream-session
equality.

Stream creation selects the next generation read-only under the object-PG lock.
The primary builders for Direct PUT generation reservation, stream creation and
stream append keep one deferred, query-only SQLite snapshot across their reads.
They end that snapshot before returning a candidate and recheck the captured
admission; error/unwind also rolls it back and restores the connection mode.
The scope performs no write transaction or fsync, and does not authorize pending
installation or preserve a candidate across later state changes. No network or
physical shard IO belongs inside it. Staging-descriptor activation remains
separate, coordinated work in the durable-cleanup ownership plan.

The single `CreateStreamUpload` apply transaction commits the explicit generation
reservation/high-water, session, and applied log together on each replica. PUT
and UploadPart no longer publish an earlier `ReserveObjectGeneration`; Direct PUT
still does. A rejected creation cannot leave a generation-only reservation, and
request failure must not release a generation owned by an ambiguously applied
creation. Safe abandonment of `CreateStreamUpload` therefore has no generation-
release derivative, on either ordinary or historical recovery routes. An
independent reservation with the same ID must survive failed creation cleanup.
Stream abort removes the generation with the session in its apply transaction;
scavenging and bucket deletion must not publish an additional generation release.
Direct PUT terminal abandonment still has its separate generation-release
obligation. Although streamed PUT uses the same `CommitDirectPutObject` payload,
its canonical stream-create proof does not authorize that standalone derivative;
normal and authorized-recovery cleanup leave its generation with the session for
atomic abort/expiry cleanup. These are the only two accepted terminal operation
classes; replica apply and replay cleanup reject unrelated or unknown proof
operations before mutation, even with an otherwise matching reservation.
Abandonment also rejects unknown classes before recording a tombstone, releasing
reservations, removing the pending slot or deleting payload. Invalid classification
is not the valid stream no-derivative case: leave the pending command and all its
effects intact for diagnosis, rather than orphaning its generation reservation.
Central application of standalone reserve/release
commands rejects any existing stream owner under the same reservation ID. This
checks durable ownership even when a decoded command supplies matching subjects
or a predecessor claims the Direct PUT proof kind.

Builders reject a pre-existing reservation under the requested session
ID. Apply independently requires reservation absence inside the creation
transaction whenever the session is absent, including for decoded recovery
commands. Only an existing exact session may use the idempotent branch, which
verifies the command-selected generation. Contenders
still return to the snapshot-sensitive owner loop to revalidate authority and
rebuild, using the existing publisher IDs/classes and bucket-proof disposition.
This reduces healthy stream creation by one object-PG command, not by a claimed
measured fsync count. It does not implement retained cleanup ownership after
session removal.

We considered replacing the allocator floor with non-state allocation. A
random `u64` segment VID is not strong enough to treat collisions as
impossible, and checking for collisions by scanning existing segment rows would
be more expensive and less direct than using SQLite's counter update. Deriving
the VID from the command id/log index would be exact, but it requires creating
or reserving the append command before payload shards are written. That would
need a larger protocol change so a pending append command cannot be applied by
another drainer before payload acks exist.

For now, keep the SQLite allocator floor. If this is revisited, prefer either a
larger collision-resistant opaque segment identity or a command-protocol change
that makes command-derived VIDs safe before payload write. Do not switch to
best-effort random `u64` allocation without an explicit collision model.

## Local Locks And Caches

Process-local locks may still protect Rust object safety, SQLite connection
use, or performance caches. They must not be the authority for logical command
ordering.

Allowed local mechanisms:

- a mutex around a `PgStore` handle for per-connection safety
- immutable or recomputable caches, such as EC write-state caches
- digest fast paths guarded by durable revision checks and fail-closed restart
  validation

Not allowed as logical authorities after Phase 9.2:

- in-memory command log index allocators
- in-memory pending command maps
- process-local metadata command apply locks
- process-local stream segment VID allocators

## Phase 9.2 Exit Criteria

Phase 9.2 is complete when:

1. command log index allocation is durable and PG-primary owned
2. every PG has at most one unresolved durable command slot
3. pending command convergence is visible after process restart and from a
   second coordinator handle
4. command application ordering does not depend on a process-local apply lock
5. stream segment VID allocation is durable or command-owned
6. tests cover two handles racing on the same PG for command allocation,
   pending command convergence, and stream append segment allocation
7. tests cover two different buckets on the same PG trying to create
   overlapping pending commands, proving the slot is PG-scoped rather than
   `(PG, bucket)` scoped
8. tests cover the zero-replica-apply abandon boundary and the nonzero-apply
   convergence boundary
9. failure-injection tests prove a replica cannot persist materialized metadata
   without the matching command-log record, or a command-log record without the
   matching materialized metadata mutation

## Non-Goals

Phase 9.2 does not need to maximize intra-PG concurrency. It also does not need
to implement remote RPC, peering, repair, or placement changes. Those later
phases can rely on the command stream being a durable, strictly ordered PG
coordination primitive.
