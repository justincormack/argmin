# Multihost Phase 11 Stabilisation Plan

Status: active

## Context

Phase 11 of [`multihost-transition-plan.md`](multihost-transition-plan.md) delivered the
single-authority control plane, PG peering, heartbeats, shard repair, PG backfill,
metadata migration, and command-log retention. Phase 12 (replicated control plane) has
started on top of it.

Soak testing keeps finding correctness issues in the Phase 11 layers even after the phase
was nominally closed. A review of the fix commits landing between the Phase 12 raft
commits shows they cluster into a small number of recurring failure modes, each fixed
*reactively at one call site* rather than *structurally across the class*:

1. **Durable state vs cached denormalisation desync.** A mutator forgot to refresh or
   invalidate a cache that mirrors durable SQLite state.
   - `c828f3a5 Refresh metadata digest after finalized bucket delete`
   - `c5776092 Clean terminal pending slots before heartbeat`
   - `791b409c Clean orphan metadata pending slots during heartbeat`
2. **"Validate the related field, forget the actual field."**
   - `4ec9a4df Reject future epoch heartbeats`
   - `6f9d1605 Reject active imported provenance without epoch`
   - `746bbb8c Harden metadata checkpoint startup races`
3. **Proof/boundary validation added incrementally.**
   - `9b577d18` -> `5d938f5f` -> `23713bb7` -> `f72ac9d4` -> `297fe9bb`
4. **Transition code copies fields without re-checking invariants.**
   - `6f9d1605`, `746bbb8c`
5. **Backlog stalls the worker that bounds the backlog.**
   - `932ae356 Keep metadata checkpoints running under backlog`

The unifying diagnosis: Phase 11's correctness model is internally consistent but relies
on a web of manual invariants (cache refresh, pending-slot cleanup, field-copy correctness,
proof exactness) that are not enforced by the type system or by a recovery gate. Every soak
bug has been one missed manual step.

This plan holds structural slices that close whole *classes* of these bugs rather than
individual instances. Slices are ordered by leverage.

## Scope

In scope:

- startup/recovery gates that convert "stale-cache-until-next-heartbeat" windows into
  fail-closed-at-open guarantees
- making cache-maintenance invariants harder to violate by construction
- centralising proof/boundary validation that is currently re-derived per call site
- property/model tests that preserve invariants under fault, restart, and concurrency

Out of scope:

- the Phase 12 replicated control plane itself; this plan hardens the state shape and
  storage-layer invariants that Phase 12 inherits. Where a slice is a prerequisite for
  Phase 12 safety it is noted.

## Relationship to Phase 11 exit criteria

This plan does not reopen Phase 11. The Phase 11 exit criteria remain met. These slices
remove the residual latent risk that the exit-criteria tests sample but cannot exhaustively
cover, as anticipated by Phase 11 work items 10-13 (deterministic fault injection,
correctness soak, property/model tests).

## Slice 1: PG Store Startup/Recovery Gate

### Goal

Make opening a `PgStore` a real recovery boundary: a PG that crashed mid-apply, that
accumulated an orphaned pending command slot, or whose cached digest diverged from its
materialised rows must be reconciled or rejected **before any request is served**, instead
of waiting for the next heartbeat tick to paper over the inconsistency.

### Background

A restarted `PgStore` currently serves requests on whatever denormalised state survived the
crash. Three recent fixes show the gap:

- `c828f3a5`: `delete_finalized_bucket` modified digest tables outside the command-apply
  path and forgot to refresh `metadata_command_replica_state.state_digest` on commit and to
  invalidate `clean_metadata_digest_revision` on rollback. A rolled-back transaction left
  the cache claiming "clean" while the DB had rolled back.
- `c5776092`: the heartbeat read a pending command slot that had already been applied (a
  *terminal* slot) and reported `has_pending_metadata_command: true` forever, sticking the
  PG in `Peering`.
- `791b409c`: the heartbeat read a pending slot from a *different* (stale) epoch and again
  reported a phantom pending command.

Both pending-slot fixes are in `crates/storage/src/node.rs::pg_heartbeat_observation`
(around `node.rs:1200`) — they are reactive, heartbeat-only cleanups. The `c828f3a5` digest
fix is local to one mutator.

### Current state (verified at `62ecf6b6`)

- `PgStore::open` (`crates/storage/src/pg_store.rs:405`, bootstrap at `:466-468`) calls
  only `ensure_metadata_digest_bootstrap` (`command_log.rs:879`) and
  `ensure_metadata_command_replica_state` (`command_log.rs:1089`). No replay validation,
  no terminal/orphan slot cleanup, no digest recompute.
- `SharedStorageNode::open` (`node.rs:493`) loops over `PgStore::open` and inherits the
  same lack of recovery.
- `StorageNodeServer::bind` (`storage_node_server.rs:964`) — the actual production
  multihost restart path — performs no recovery validation at all. A storage node restarting
  independently serves requests on unvalidated PG state until the first heartbeat tick.
- A partial open-time cleanup existed but was narrow:
  `clean_terminal_primary_pending_slot_on_open` has now been removed, and terminal pending
  slots are cleaned by the shared replay validation/recovery path. The remaining
  `converge_in_flight_metadata_command_on_open` local-cluster path is for genuine in-flight
  primary commands that need to be applied to every replica before replay validation can
  prove convergence.
- `metadata_state_digest_mismatch` (`command_log.rs:3659`) runs only from
  `metadata_command_acceptance` (`:3258`, `:3298`) — i.e. on the command-apply path. It is
  never run at open.
- `record_metadata_command_applied` (`command_log.rs:3303`) inserts into
  `metadata_command_log` and advances replica state but does **not** remove the matching
  pending slot; removal is left to callers (`remove_pending_metadata_command_slot` at
  `:2201`) or to reactive reconciliation. Any crash between apply and removal orphans a
  terminal slot.

### Work items

Status summary:

- **Completed:** recovery semantics, opened-store recovery invocation, cached-table drift
  detect-and-repair, pending-slot finalization, divergent open-time cleanup consolidation,
  storage-node bind coverage for the main recovery classes, the generic commit-fault hook,
  and direct commit-failure coverage for the explicit multi-statement mutator inventory.
- **Completed:** the remaining digest-affecting `PgMetadataStore` surface has been
  classified as single SQLite statement maintenance paths or read-before-single-write
  helpers. These stay under ordinary digest-consistency coverage and do not need the
  commit-failure hook unless they are later expanded into multi-write transactions.
- **Remaining Slice 1 work:** no known implementation item remains. Treat future findings
  as regressions against the completed startup/recovery gate, or reopen this slice if a
  missed multi-statement mutator is found.

Progress update:

- `PgStore::recover` exists as an opened-store recovery boundary, and both
  `StorageNodeServer::bind` and the local-cluster builder now call
  `SharedStorageNode::recover_pg_metadata_command_state(node_id)` before serving PG state.
  The control-plane-managed storage-node startup heartbeat path also runs the same recovery
  immediately after its pre-bind `SharedStorageNode` open, before reading heartbeat state.
  That pre-bind open now happens under a `StorageNodeDataDirGuard` that is transferred into
  `StorageNodeServer::bind_with_data_dir_guard`, so startup recovery cannot mutate PG state
  before the storage-node data-dir lock is held.
- Recovery now repairs cache-only per-table digest drift after replay validation; a focused
  regression corrupts only `metadata_table_digests`, verifies recovery refreshes the cache,
  and then applies a later metadata command without tripping dirty cached digest detection.
- Wiring recovery into `bind` exposed test fixtures and raw SQL setup paths that bypassed
  command apply and left `metadata_command_replica_state.state_digest` stale. Those fixtures
  now either use the command-apply path or explicitly refresh the digest after direct raw
  setup. This is test-only cleanup; production recovery still fails closed on stale digest
  state.
- Multipart initiator identity is now required across storage records, metadata commands,
  RPC payloads, and response/authz structs. The old nullable schema/open-time owner-identity
  migration and storage-side `None => owner` fallback were removed because they could mutate
  materialised rows on reopen and make caller semantics unclear.

1. **Define recovery semantics explicitly.** **Completed.** Document, in
   [`guides/storage-cluster-invariants.md`](../guides/storage-cluster-invariants.md), that
   opening a PG store is a recovery boundary and state which anomaly classes fail closed
   (possible corruption: hash-chain break, digest mismatch, forked log) versus which are
   reconciled (benign crash leftovers: terminal pending slot, epoch-mismatched orphan
   slot). The heartbeat path currently reconciles silently; recovery should make this
   distinction deliberately.

2. **Recovery is a method on an opened store, invoked by node-identity-owning callers.**
   **Completed.**
   Recovery needs `node_id`, which neither `PgStore` nor `SharedStorageNode` owns.
   `SharedStorageNode::open` / `open_with_default_ec_shape` (`node.rs:489`/`493`) take only
   data dir, pg ids, and EC shape; node identity is supplied per-call (e.g.
   `pg_heartbeat_observation` takes `node_id` at `node.rs:1195`). Threading `NodeId` into
   `SharedStorageNode::open` would be churn for no other benefit, so recovery belongs at the
   layer that already has node identity:
   - add `PgStore::recover(&self, ctx: PgStoreRecoveryContext { node_id })` as a method on
     an already-opened store, not a constructor. The epoch is read internally from
     `metadata_command_replica_state().cluster_epoch` and is **never** passed in. Passing an
     external authority/config epoch (the `746bbb8c` mistake) would defeat the orphan check
     and must be rejected;
   - `StorageNodeServer::bind` (`storage_node_server.rs:964`) calls `recover` per PG after
     opening the node, using `config.node_id`;
   - the local cluster builder calls `recover` per PG per node, using the `NodeId` it
     assigns each `LocalNodeStore`;
   - `PgStore::open` and `SharedStorageNode::open` stay raw (no recovery, no node_id).

   Per opened PG, `recover` runs, in immediate transactions and in this order. The order is
   load-bearing and mirrors the heartbeat path (`node.rs:1203-1217`), which already solves
   the sequencing hazard that affects the naive order:
   1. read the stored replica epoch from `metadata_command_replica_state().cluster_epoch`;
   2. run `clean_epoch_mismatched_orphan_pending_metadata_command_slot(node_id, stored_epoch)`
      (`command_log.rs:2049`). This **must** precede replay validation:
      `validate_metadata_command_replay_state_with_pending_cleanup` calls the epoch-checked
      `pending_metadata_command_slot(node_id, cluster_epoch)` at `command_log.rs:2856`,
      which returns `StaleMetadataOperation` when the slot's epoch differs from the stored
      epoch. Running validation first would error out on an epoch-mismatched orphan before
      this cleanup could run, so the orphan would never be reconciled and recovery would fail
      rather than heal;
   3. run `validate_metadata_command_replay_state` with
      `PendingMetadataCommandSlotCleanup::CleanTerminal` (`command_log.rs:2362`) to
      reconcile terminal pending slots and validate the command-log hash chain. This step
      also performs the materialised digest verification (see work item 2b);
   4. assert that any surviving pending slot references a live, in-order log entry.

2b. **Cached-table refresh and per-table diagnostics on top of replay validation.**
   **Completed for Slice 1.** Replay
   validation (step 2.iii) already verifies `state.state_digest` against a full materialised
   recompute — `metadata_state_digest()` at `command_log.rs:2882`, which calls
   `metadata_table_digest()` (`command_log.rs:4095`/`4415`, a canonical row scan) per table.
   So Slice 1's primary digest correctness check is covered **by ensuring replay validation
   runs** (the ordering fix above), not by a separate scan. What replay validation does NOT
   do is rewrite the cached `metadata_table_digests` rows or pinpoint which table diverged.
   On top of step 2.iii, recovery does both — but **in detect-then-repair order**, because
   `refresh_all_metadata_table_digests()` overwrites the cached per-table digests from
   materialised rows and would destroy the drift evidence if run first:
   1. **detect** — run the per-table cached-vs-materialised comparison
      (`cached_metadata_table_digest()` at `command_log.rs:4518` vs `metadata_table_digest()`
      at `command_log.rs:4415`, reusing the `test_metadata_digest_table_mismatches` pattern
      at `command_log.rs:3645`). This catches cached-table drift that xor-cancels out of the
      state digest and would otherwise pass step 2.iii. Record/trace any mismatch with the
      table name so the divergent table is identifiable;
   2. **repair** — run `refresh_all_metadata_table_digests()` (`command_log.rs:4476`) to
      rewrite the cached per-table digests from materialised rows, so a drifted
      trigger-maintained cache cannot poison the next mutation (the normal apply path reads
      that cache). Replay validation only marks the in-memory clean marker
      (`mark_metadata_state_digest_clean` at `command_log.rs:2892`); it does not repair the
      cached rows.

   Failure politics: step 2.iii returns `MetadataStateDigestMismatch` and fails closed on
   genuine corruption (stale trigger producing a wrong stored digest, or a rolled-back
   transaction). The detect step runs only after step 2.iii has already confirmed the stored
   digest, so a mismatch here is cache-only drift with provably-correct materialised state.
   Slice 1 uses warn-and-repair with a prominent trace: the materialised state has already
   been proven correct, so recovery refreshes the cached rows before serving. Slice 2's
   trigger-body check remains the place to detect the stale-trigger root cause and decide
   whether that should become fail-closed.

3. **Generalise the existing commit-fault hook.** **Completed.** The
   `fail_next_delete_finalized_bucket_commit`
   flag (`pg_store.rs:387`) is a per-mutator test hook. Lift it to a generic
   `fail_next_metadata_txn_commit` hook so the crash-recovery property test (work item 6)
   can inject a commit failure into any digest-affecting transaction, not just finalized
   bucket delete.

   Progress update: the generic hook now exists on `PgStore` and is consumed by
   `with_immediate_txn` immediately before `COMMIT`. It rolls back, invalidates the clean
   digest revision, and returns the caller's normal commit-context error. The old
   delete-finalized hook still exists for its legacy targeted regression, but the generic
   hook now also runs at hand-written metadata transaction commit points in
   `metadata.rs`. Single-statement mutators with no explicit transaction, such as stream
   upload session create, have no injected commit window by design; the Slice 1 inventory
   now classifies those as SQLite-atomic maintenance paths unless a future change expands
   them into multiple writes.

4. **Make pending-slot removal transactional at the safe finalization boundary.**
   **Completed.** A first
   attempt to make `record_metadata_command_applied` delete the matching pending slot proved
   too early: that function is called per replica during fanout, and clearing the primary
   slot as soon as the primary records the command breaks partial-fanout recovery and
   reissue paths. The pending slot must survive until the acting set has converged or a
   recovery/drain path has decided the terminal command can be finalized.

   The safe hardening is therefore narrower:
   - keep `record_metadata_command_applied` as per-replica log/state recording only;
   - make `remove_pending_metadata_command_slot` transactional, with an exact full-identity
     match (`cluster_epoch + pg_id + log_index + command_checksum + command_bytes +
     scope_bucket`) and a terminal log-entry check before removal;
   - continue using recovery validation (`CleanTerminal`) to reconcile terminal slots left
     by crashes before this explicit cleanup point.

   This keeps the legitimate "applied on some/all replicas but still pending on the primary"
   intermediate state representable, while tightening the actual cleanup operation.

5. **Consolidate the divergent open-time cleanups.** **Completed.** Once
   `PgStore::recover` reconciles
   terminal and orphan slots for every role and every caller:
   - delete `clean_terminal_primary_pending_slot_on_open` (`cluster/local.rs:3868`) and
     fold its bucket-write-reservation release into the recovery pass (or a documented
     successor), so the local-cluster build and the storage-node server no longer have
     different recovery semantics;
   - confirm `converge_in_flight_metadata_command_on_open` (`cluster/local.rs:3840`) is
     still needed for genuine in-flight (non-terminal) commands and is not duplicating
     recovery-pass work.

   Progress update: terminal pending-slot cleanup is now consolidated in
   `validate_metadata_command_replay_state`/`PgStore::recover`; the old
   `clean_terminal_primary_pending_slot_on_open` helper has been removed. Local-cluster open
   still releases command-owned bucket-write reservations for terminal or converged commands
   before replay validation removes the terminal slot, because reservation rows live on the
   bucket PG and require the cluster route context. `converge_in_flight_metadata_command_on_open`
   remains necessary for genuine primary-pending commands that have not yet been applied to
   every replica.

6. **Crash-recovery property test for every digest-affecting mutator.**
   **Completed for explicit multi-statement mutators.** Using the
   generalised hook from work item 3, for each metadata-mutating transaction: inject a
   commit failure, reopen the PG through `PgStore::open` then `pg.recover(ctx)`, and assert
   (a) per-table materialised digests match the cached `metadata_table_digests` rows and
       `metadata_command_replica_state.state_digest` (not just cached-vs-cached),
   (b) no orphaned pending slots remain,
   (c) the command-log hash chain is intact,
   (d) `metadata_command_replica_state` matches materialised state,
   (e) the PG can accept the next command without a spurious retry/drain cycle.
   This single harness would have caught `c828f3a5`, `c5776092`, and `791b409c`.

   Progress update: a reusable harness now covers representative generic-transaction
   mutators (`create_bucket`, bucket versioning, bucket ACL, bucket encryption, bucket
   Object Lock, public access block put/delete, ownership controls put/delete, ABAC,
   bucket subresource put/delete, `mark_bucket_deleting`, and create multipart upload),
   explicit bucket lifecycle transactions (`delete_finalized_bucket`,
   metadata-command reservation release, expired drain clear, and drain heartbeat), plus
   representative hand-written metadata transactions (object metadata put, object-version
   delete, object generation reservation, segmented object put, object-segment reclaim,
   multipart reclaim, multipart upload delete, direct multipart part upsert, committed
   multipart object-part manifest insertion, full multipart completion, staged multipart
   part segment upsert, streamed UploadPart finalization, and standalone stream-upload
   delete). Each case injects a commit failure, reopens and recovers the PG, checks
   cached-vs-materialised table digests and replica-state digest consistency, asserts no
   pending slot remains, asserts the PG accepts the next metadata command, and then applies
   that command. The explicit multi-statement transaction inventory is now represented by
   direct commit-failure coverage. A follow-up audit pass classified the remaining
   digest-affecting `PgMetadataStore` methods as single SQLite statement maintenance paths,
   or as read-before-single-write helpers where the pre-write read failure leaves no
   mutation to recover. Those paths do not need the commit-failure hook unless they are
   later expanded into multiple writes. This completes the Slice 1 mutator inventory.

   Remaining non-transactional groups to keep under ordinary digest-consistency coverage:
   - bucket reservation/drain/finalizer/lifecycle/reclaim claim maintenance rows,
     especially `acquire_*`, direct `release_*`/`clear_*`, claim heartbeats/error records,
     and attempt-outcome helpers;
   - object metadata maintenance helpers such as object ACL, retention, legal hold,
     object-tag put/delete, object metadata delete, and generation reservation release;
   - stream/upload maintenance helpers such as stream upload create, stream upload state,
     stream segment append, stream segment VID floor advance, MPU state changes,
     completed-upload delete, object-part delete, object-segment delete, and reclaim-row
     delete helpers.

7. **Storage-node restart coverage.** **Completed for current Slice 1 recovery classes.**
   Add a test that opens a PG through
   `StorageNodeServer::bind` (the production restart path), after a crash that orphans a
   terminal slot and a commit-failed transaction, and asserts the server does not serve a
   read until recovery has reconciled the state. This pins the path that currently has zero
   recovery.

   Progress update: `StorageNodeServer::bind` now has direct regressions for cleaning a
   terminal pending metadata command during bind recovery and for failing closed on a
   corrupted metadata state digest before serving. Bind also has a direct regression for
   repairing cache-only per-table digest drift before serving, so the production restart
   path is pinned for the same drift class as `PgStore::recover`.

### Exit criteria

1. **Completed:** `PgStore::recover(&self, PgStoreRecoveryContext { node_id })` reconciles
   epoch-mismatched orphan and terminal pending slots (in that order) and fails closed on
   materialised-digest or hash-chain mismatch. `PgStore::open` stays raw; recovery is
   invoked by node-identity-owning callers, not threaded through `SharedStorageNode::open`.
2. **Completed:** `StorageNodeServer::bind` and the local-cluster builder both call
   `recover` per PG with their node id, so a restarted storage node never serves on an
   unvalidated PG.
3. **Completed:** Pending-slot finalization is transactional and exact-match, while
   `record_metadata_command_applied` remains per-replica recording only so partial-fanout
   recovery remains correct.
4. **Completed:** The local-cluster build path and the storage-node server share one
   recovery code path; `clean_terminal_primary_pending_slot_on_open` is removed.
5. **Completed:** A crash-recovery property test covers the explicit multi-statement
   digest-affecting mutators and asserts the five post-recovery invariants above,
   including materialised-vs-cached digest agreement and that an epoch-mismatched orphan
   slot does not block recovery. The remaining digest-affecting store methods are
   classified as single SQLite statement maintenance paths or read-before-single-write
   helpers, so they remain under ordinary digest-consistency coverage rather than the
   commit-failure hook.
6. **Completed:** The reactive heartbeat cleanups (`c5776092`, `791b409c`) remain as a
   defence-in-depth heartbeat-side check but are no longer the primary correctness
   mechanism; this is documented in `guides/storage-cluster-invariants.md`.

### Out of scope for Slice 1

- SQLite trigger SQL body verification (moved to
  [`storage-upgrade-versioning-plan.md`](storage-upgrade-versioning-plan.md)). The
  materialised digest scan in work item 2b catches the *symptom* of a stale trigger
  definition at open. Trigger-body verification is part of future versioned upgrade
  support, not current Phase 11 stabilisation, because the project deliberately does not
  support opening older stores yet.
- Removing the same-epoch digest-only proof relaxation by making final bucket cleanup a
  metadata command, tightening imported metadata-transfer proof ordering, and deleting
  tests that rely on out-of-band digest mutation (tracked as Slice 3).
- Control-plane open->save->reload transition property tests (tracked as Slice 4).

## Tracked future slices

These are recorded here so the backlog is visible; each will be expanded into a full slice
when prioritised. They correspond to the remaining findings from the Phase 11 review.

- **Moved out:** Trigger SQL body verification now lives in
  [`storage-upgrade-versioning-plan.md`](storage-upgrade-versioning-plan.md). It should be
  implemented only as part of a deliberate upgrade-support phase, after legacy format
  cleanup and baseline versioning are in place.

- **Slice 3: Remove digest-only metadata proof escape hatches.**

  **Goal:** a metadata proof that changes `state_digest` must also be explained by
  metadata-command log progress, except for explicitly imported metadata-transfer state with
  a separately ordered provenance proof. The same-epoch
  `allow_same_epoch_digest_only_progress` branch should be removed, not narrowed. A digest
  change at the same `applied_log_index`/`applied_log_hash` is otherwise an uncharacterised
  out-of-band mutation and should fail closed.

  **Why this matters:** the current relaxation was added to keep peering/transfer moving
  when final bucket deletion mutates materialised metadata after the logged
  `MarkBucketDeleting` command. That keeps the system live, but it also creates a generic
  "same log, different digest" escape hatch. It does not encode the shape of the work that
  happened, so unrelated divergence can hide behind the same proof rule and becomes hard to
  verify.

  Work items:
  - **Final bucket cleanup becomes a metadata command.** Add a terminal command, e.g.
    `DeleteFinalizedBucket`, after the finalizer proves the bucket is deleting, empty, and
    reclaimed. Applying that command deletes the finalized bucket metadata rows. It must
    carry enough bucket identity/generation/delete-execution provenance to be stale-safe
    across bucket recreation, be idempotent under replay/recovery, and clear/finalize its
    pending slot through the normal command-log machinery. Once this lands, finalized
    bucket cleanup advances `applied_log_index`/`applied_log_hash` instead of changing only
    `state_digest`.
  - **Remove direct production digest refresh for finalized bucket cleanup.** The current
    `delete_finalized_bucket` path is the named production exception that refreshes
    `metadata_command_replica_state.state_digest` without appending a metadata command.
    Replace production use of that out-of-band cleanup with the terminal command. Any
    remaining direct helper should be test-only or private recovery scaffolding with a name
    that makes it impossible to call from serving paths accidentally.
  - **Tighten imported metadata-transfer proofs.**
    `metadata_proof_satisfies_fenced_transfer_source_floor` currently accepts a
    non-zero-hash different-digest proof when `active_metadata_transfer_imported` is set.
    Keep the imported-transfer case, but require explicit ordering/provenance: the imported
    proof must be tied to the source route/import epoch and must satisfy log-index/hash
    ordering rather than relying on "different digest" as evidence.
  - **Delete test dependencies on digest-only mutation.** Audit tests and fixtures that
    create same-log/different-digest state by directly mutating materialised metadata or by
    refreshing digest rows without a command. Convert them to either apply the new terminal
    metadata command, construct imported-transfer provenance explicitly, or assert that the
    state is rejected. The remaining tests should not require
    `allow_same_epoch_digest_only_progress` to pass.
  - **Remove the relaxation from proof validation.** Delete the
    `allow_same_epoch_digest_only_progress` branch in
    `metadata_proof_satisfies_active_primary_observation_floor_impl` and make same-epoch,
    same-log, different-digest active-primary observations fail closed.
  - **Add divergent-replica coverage.** Add a property/regression test that applies the same
    command log to two replicas, injects one out-of-band materialised metadata mutation on
    one replica, and asserts peering/metadata-transfer proof completion rejects it. Add a
    positive test showing finalized bucket cleanup now succeeds because the terminal command
    advances the command log, not because digest-only progress is allowed.

  Exit criteria:
  1. Production finalized bucket cleanup is represented by a metadata command log entry.
  2. Imported metadata-transfer digest differences are accepted only with explicit ordered
     provenance.
  3. No production proof path accepts same-epoch, same-log, different-digest active-primary
     observations.
  4. Tests no longer depend on generic digest-only progress; they either use logged
     commands, imported-transfer provenance, or expect rejection.

- **Slice 4: Control-plane transition open/save/reload property tests.** **Completed for
  the current Phase 11 PG transition graph.** The `6f9d1605` "second restart fails" bug is
  the canonical shape: transition code copies active fields verbatim into a stricter-shaped
  peering target. The heartbeat/control-plane model proptest now includes file-backed
  authority restarts as first-class operations, so randomly generated heartbeat, acting-set,
  lease-expiry, and peering-completion sequences must survive `save -> open -> validate ->
  continue`. A focused deterministic regression also walks the PG graph through
  `Peering -> Active`, active restart, overlap acting-set migration, non-overlap metadata
  transfer, imported active completion, and imported active restart, reopening the
  file-backed authority at each load-bearing boundary. This pins proof-floor preservation,
  transfer source route provenance, imported transfer markers, and restart epoch bumping at
  the parser/runtime boundary.

- **Slice 6: Deterministic fault-injection matrix.** The smoke suites are now mostly
  finding `DeleteBucket`, timeout, retry, and whole-process robustness issues. They are
  still useful, but they are too slow and non-deterministic to be the primary way we find
  partial-state correctness bugs in ordinary object/MPU/read paths. Promote the existing
  token-scoped failpoint machinery into an explicit coverage matrix. Each test should pause
  exactly one operation at a named boundary, advance the runtime map or PG state, release
  that operation, and then assert exact metadata, pending-slot, reservation, shard/ack, and
  visibility outcomes.

  Boundary classes:
  - **B1: after payload shard write, before metadata publish.** Payload is durable but the
    object/part metadata command has not been applied.
  - **B2: after reservation or pending-command install, before metadata apply.** Bucket
    write proof, drain, or metadata pending slot exists and must be cleaned or completed
    exactly once.
  - **B3: after metadata apply, before response/cleanup.** The command committed, but the
    caller may see a lost response or run cleanup under a newer route.
  - **B4: cleanup/finalization.** Best-effort cleanup must use retained placement/proof
    routes and must not leak or delete the wrong generation.
  - **B5: route/PG state transition.** The operation starts with one route/primary and
    finishes after a runtime-map refresh, acting-set change, or Peering transition.

  Coverage matrix:

  | Path | Positive crossing coverage | Stale-primary / Peering fail-closed coverage | Remaining Slice 6 gap |
  | --- | --- | --- | --- |
  | Direct PUT / overwrite | Covered locally at B1/B2, partial-apply/pending-command replay, and exact B3 committed-response-loss retry for both first PUT and overwrite reclaim identity: `S6-DP1`, `S6-DP4`, `S6-DP5`. | Covered locally and through installed Unix clients for stale epoch and control-plane-driven Peering: `S6-DP2`, `S6-DP3`. | No current Slice 6 direct-PUT gap named. |
  | Streaming PutObject | Partially covered: stream session pinning and command-conflict cleanup are named in `S6-SP1`, and storage command retry/finalize cleanup is named in `S6-SP2`. | Covered locally and through installed Unix clients for real-Peering `CommitStreamPut` in `S6-SP4` and `S6-SP5`; remote stale segment append/commit is named in `S6-SP3`. | Add terminal checksum/final chunk failure cleanup and abort cleanup across B1/B2/B4/B5. |
  | DeleteObject | Covered locally before metadata apply and partial/terminal command replay: `S6-DEL1`, `S6-DEL4`. | Covered locally and through installed Unix clients for stale epoch and real Peering: `S6-DEL2`, `S6-DEL3`. | Add exact B3 committed-delete response-loss/retry coverage. |
  | Object metadata mutations: tags/legal hold/retention/ACL-shaped path | Covered for tags, legal hold, and retention locally before shared `PutObjectMetadata` apply across an epoch change: `S6-META1`. ACL is the same serialization shape but not named yet. | Covered for shared object-metadata update locally and through installed Unix clients for stale epoch and real Peering: `S6-META2`, `S6-META3`. | Decide whether ACL needs one representative test or remains covered by shared command-shape evidence. |
  | CopyObject destination publish | Covered locally before destination `CommitDirectPutObject` across a runtime-map epoch change: `S6-COPY1`. | Covered locally and through installed Unix clients for real Peering: `S6-COPY2`, `S6-COPY3`. | Add B3 committed-copy response-loss/idempotent retry coverage if missing. |
  | CompleteMultipartUpload | Covered locally before multipart commit apply, matching pending completion, and terminal cleanup after already-recorded completion: `S6-CMP1`, `S6-CMP4`. | Covered locally and through installed Unix clients for stale epoch and real Peering: `S6-CMP2`, `S6-CMP3`. | No current Slice 6 CompleteMultipartUpload gap named. |
  | AbortMultipartUpload | Covered for coordinator route-map pinning, command-conflict mapping, storage-level partial apply/reopen, matching pending abort, zero-apply retry, and API-level second-abort behavior: `S6-ABORT1`, `S6-ABORT2`, `S6-ABORT6`. | Covered locally and through installed Unix clients for real-Peering stale-primary abort rejection in `S6-ABORT4` and `S6-ABORT5`; stale upload-row and multipart-trace stale snapshot coverage exists in `S6-ABORT3`. | Add exact B3 committed-abort response-loss/retry coverage if the intended retry semantics are stronger than API-level second-abort behavior. |
  | UploadPart stream finalization | Covered locally before `CommitStreamPart` apply across an epoch change, including UploadPartCopy-shaped copied segments: `S6-UPF1`; storage retry/finalize cleanup is named in `S6-UPF2`. | Covered by installed Unix stale/Peering paths for UploadPart and UploadPartCopy finalization: `S6-UPF3`. | Add B3 committed-part response-loss/retry coverage if missing. |
  | UploadPart stream-session creation | Covered by coordinator route-map pinning and storage create retry/reservation tests: `S6-UPC1`, `S6-UPC2`. | Covered through installed Unix stale/Peering paths with no session, segment rows, pending command, or reservation leak: `S6-UPC3`. | Add a minimal B2 pending-slot/reservation crossing test if the existing command tests do not hit it precisely enough. |
  | GET/HEAD payload reads | Covered locally after metadata snapshot and by installed-Unix retained-route read: `S6-READ1`, `S6-READ2`. | Covered for object metadata PG Peering fail-closed: `S6-READ3`. | Add an explicit malformed/stale current-route negative where a current-route payload read cannot accidentally succeed after disjoint placement. |
  | Object and version LIST | Covered locally across route-map swaps during listing and pagination, including delimiter/common-prefix continuation: `S6-LIST1`; UAT route-change smokes cover process-level retained-list behavior. | Covered for composite/listing Peering fail-closed at storage level: `S6-LIST2`; read/list object metadata PG Peering fail-closed is also in `S6-READ3`. | Add a deterministic multi-object-PG LIST route-change matrix if pagination coverage does not already span object-PG movement. |
  | DeleteBucket begin/finalize | Covered by a separate bucket-delete hardening plan and many recent regressions. | Partially covered, but recent soak failures show this is still the largest open correctness/robustness area. | Keep this out of the generic matrix except for shared failpoint API reuse; track durable attempt/adoption/finalizer interleavings in the bucket-delete plan. |
  | Control-plane PG transitions | Slice 4 covers `open -> mutate -> save -> reload -> continue` for current transitions. | Stale runtime-map and stale authorization fail-closed coverage exists. | Slice 7 handles lost-response/check-applied behavior for mutating control-plane RPCs. |

  Evidence inventory:
  - `S6-DP1`: `put_object_epoch_change_before_metadata_apply_commits_once_on_pinned_route`,
    `overwrite_object_epoch_change_before_metadata_apply_commits_once_on_pinned_route`
    in `crates/server-core/src/coordinator/core_tests.rs`.
  - `S6-DP2`: `non_current_epoch_direct_put_commit_fails_closed_and_cleans_unowned_state`,
    `control_plane_peering_direct_put_old_primary_fails_closed_and_cleans_unowned_state`
    in `crates/storage/src/cluster/local/tests/direct_put.rs`.
  - `S6-DP3`: `non_current_epoch_unix_direct_put_commit_fails_closed_and_cleans_remote_state`,
    `control_plane_peering_unix_direct_put_old_primary_fails_closed_and_cleans_remote_state`
    in `crates/storage/src/cluster/local/tests/unix_clients.rs`.
  - `S6-DP4`: `direct_put_metadata_command_retry_reuses_pending_partial_replica_command`
    in `crates/storage/src/cluster/local/tests/multipart_completion.rs`, plus
    `direct_put_retry_converges_pending_partial_metadata_command` in
    `crates/server-core/src/coordinator/core_tests.rs`.
  - `S6-DP5`: `direct_put_committed_response_loss_retry_returns_existing_commit`; `direct_put_overwrite_committed_response_loss_retry_preserves_reclaim_generation`
    in `crates/storage/src/cluster/local/tests/direct_put.rs`.
  - `S6-SP1`: `large_put_object_pins_runtime_map_after_stream_session_create`,
    `stream_put_begin_request_maps_command_log_conflict_to_operation_aborted`,
    `stream_put_finalize_request_maps_command_log_conflict_to_operation_aborted`,
    and `stream_abort_request_maps_command_log_conflict_to_operation_aborted`
    in `crates/server-core/src/coordinator/core_tests.rs`.
  - `S6-SP2`: `stream_put_create_partial_apply_retry_reuses_existing_session`,
    `stream_put_finalize_pending_drain_cleans_terminal_stream_session`,
    `stream_put_finalize_matching_pending_install_race_returns_success`,
    and `failed_stream_put_finalize_is_scavenged_without_visibility_or_orphans`
    in `crates/storage/src/cluster/local/tests/stream_commands.rs` and
    `crates/server-core/src/coordinator/multipart_stateful_tests.rs`.
  - `S6-SP3`: `non_current_epoch_unix_stream_append_commit_fails_closed_and_cleans_remote_state`
    in `crates/storage/src/cluster/local/tests/unix_clients.rs`.
  - `S6-SP4`: `control_plane_peering_stream_put_finalize_old_primary_fails_closed_and_preserves_staging`
    in `crates/storage/src/cluster/local/tests/stream_commands.rs`.
  - `S6-SP5`: `control_plane_peering_unix_stream_put_finalize_old_primary_preserves_remote_staging`
    in `crates/storage/src/cluster/local/tests/unix_clients.rs`.
  - `S6-DEL1`: `delete_object_epoch_change_before_metadata_apply_commits_once_on_pinned_route`
    in `crates/server-core/src/coordinator/core_tests.rs`.
  - `S6-DEL2`: `non_current_epoch_object_delete_fails_closed_without_mutation`,
    `control_plane_peering_object_delete_old_primary_fails_closed_without_mutation`
    in `crates/storage/src/cluster/local/tests/object_commands.rs`.
  - `S6-DEL3`: `non_current_epoch_unix_object_delete_fails_closed_without_remote_mutation`,
    `control_plane_peering_unix_object_delete_old_primary_fails_closed_without_remote_mutation`
    in `crates/storage/src/cluster/local/tests/unix_clients.rs`.
  - `S6-DEL4`: `object_delete_metadata_command_retry_reuses_pending_partial_replica_command`
    and `object_delete_exact_pending_retry_converges_partial_exact_conflict` in
    `crates/storage/src/cluster/local/tests/object_commands.rs`.
  - `S6-META1`: `put_object_tags_epoch_change_before_metadata_apply_commits_once_on_pinned_route`,
    `put_object_legal_hold_epoch_change_before_metadata_apply_commits_once_on_pinned_route`,
    and `put_object_retention_epoch_change_before_metadata_apply_commits_once_on_pinned_route`
    in `crates/server-core/src/coordinator/core_tests.rs`.
  - `S6-META2`: `non_current_epoch_object_metadata_update_fails_closed_without_mutation`,
    `control_plane_peering_object_metadata_update_old_primary_fails_closed_without_mutation`
    in `crates/storage/src/cluster/local/tests/object_commands.rs`.
  - `S6-META3`: `non_current_epoch_unix_object_metadata_update_fails_closed_without_remote_mutation`,
    `control_plane_peering_unix_object_metadata_old_primary_fails_closed_without_remote_mutation`
    in `crates/storage/src/cluster/local/tests/unix_clients.rs`.
  - `S6-COPY1`: `copy_object_epoch_change_before_metadata_apply_commits_once_on_pinned_route`
    in `crates/server-core/src/coordinator/core_tests.rs`.
  - `S6-COPY2`: `control_plane_peering_copy_object_destination_old_primary_fails_closed_and_cleans_staging`
    in `crates/storage/src/cluster/local/tests/direct_put.rs`.
  - `S6-COPY3`: `control_plane_peering_unix_copy_object_destination_old_primary_cleans_remote_staging`
    in `crates/storage/src/cluster/local/tests/unix_clients.rs`.
  - `S6-CMP1`: `complete_multipart_epoch_change_before_metadata_apply_commits_once_on_pinned_route`,
    `complete_multipart_upload_pins_runtime_map_between_snapshot_and_commit`
    in `crates/server-core/src/coordinator/core_tests.rs`.
  - `S6-CMP2`: `non_current_epoch_multipart_completion_fails_closed_without_mutation`,
    `control_plane_peering_multipart_completion_old_primary_fails_closed_without_mutation`
    in `crates/storage/src/cluster/local/tests/multipart_completion.rs`.
  - `S6-CMP3`: `non_current_epoch_unix_multipart_completion_fails_closed_without_remote_mutation`,
    `control_plane_peering_unix_multipart_completion_old_primary_fails_closed_without_remote_mutation`
    in `crates/storage/src/cluster/local/tests/unix_clients.rs`.
  - `S6-CMP4`: `multipart_completion_drains_matching_pending_completion` and
    `already_recorded_multipart_completion_fanout_cleans_terminal_stream_uploads`
    in `crates/storage/src/cluster/local/tests/multipart_completion.rs`.
  - `S6-ABORT1`: `abort_multipart_upload_pins_runtime_map_after_auth_lookup`,
    `abort_multipart_upload_pins_runtime_map_after_bucket_summary`,
    and `abort_multipart_upload_request_maps_command_log_conflict_to_operation_aborted`
    in `crates/server-core/src/coordinator/core_tests.rs`.
  - `S6-ABORT2`: `multipart_abort_partial_apply_retry_cleans_uploaded_part_payload`,
    `multipart_abort_partial_apply_reopens_and_converges`,
    `multipart_abort_pending_install_conflict_cleans_upload_part_stream_session_and_segments`,
    and `multipart_abort_zero_apply_leaves_upload_in_progress_before_retry`
    in `crates/storage/src/cluster/local/tests/multipart.rs`.
  - `S6-ABORT3`: `authorized_multipart_abort_rejects_stale_upload_row` in
    `crates/storage/src/cluster/local/tests/multipart.rs`, plus
    `prop_multipart_trace_preserves_terminal_lifecycle_invariants` and
    `multipart_trace_exercises_bucket_delete_recreate` in
    `crates/storage/src/cluster/local/tests/multipart_trace.rs`.
  - `S6-ABORT4`: `control_plane_peering_multipart_abort_old_primary_fails_closed_without_mutation`
    in `crates/storage/src/cluster/local/tests/multipart.rs`.
  - `S6-ABORT5`: `control_plane_peering_unix_multipart_abort_old_primary_fails_closed_without_remote_mutation`
    in `crates/storage/src/cluster/local/tests/unix_clients.rs`.
  - `S6-ABORT6`: `multipart_abort_matching_pending_install_race_returns_success`,
    `authorized_multipart_abort_matching_pending_install_race_returns_success`, and
    `multipart_abort_zero_apply_leaves_upload_in_progress_before_retry` in
    `crates/storage/src/cluster/local/tests/multipart.rs`; API-level already-aborted behavior
    is pinned by `abort_multipart_upload_idempotent` in
    `crates/server-core/src/coordinator/multipart_tests.rs`.
  - `S6-UPF1`: `upload_part_finalize_epoch_change_before_metadata_apply_commits_once_on_pinned_route`
    and `upload_part_copy_epoch_change_before_metadata_apply_commits_once_on_pinned_route`
    in `crates/server-core/src/coordinator/core_tests.rs`.
  - `S6-UPF2`: `stream_part_finalize_pending_drain_cleans_terminal_stream_session`,
    `upload_part_stream_finalize_partial_apply_reopens_and_converges`,
    `upload_part_stream_finalize_finishes_terminal_pending_slot`, and
    `failed_stream_part_finalize_abort_cleanup_leaves_no_visible_part_or_orphans`
    in `crates/storage/src/cluster/local/tests/stream_commands.rs` and
    `crates/server-core/src/coordinator/multipart_stateful_tests.rs`.
  - `S6-UPF3`: `non_current_epoch_unix_upload_part_stream_finalize_fails_closed_without_remote_mutation`,
    `non_current_epoch_unix_upload_part_copy_finalize_preserves_copied_staging`,
    `control_plane_peering_unix_upload_part_finalize_old_primary_preserves_remote_staging`,
    and `control_plane_peering_unix_upload_part_copy_finalize_old_primary_preserves_remote_staging`
    in `crates/storage/src/cluster/local/tests/unix_clients.rs`.
  - `S6-UPC1`: `streaming_upload_part_pins_runtime_map_after_session_create`,
    `upload_part_copy_pins_runtime_map_after_stream_session_create`, and
    `upload_part_append_request_maps_command_log_conflict_to_operation_aborted`
    in `crates/server-core/src/coordinator/core_tests.rs`.
  - `S6-UPC2`: `upload_part_stream_create_zero_apply_reopens_and_converges`,
    `begin_upload_part_stream_pending_install_race_reruns_action`, and
    `begin_upload_part_stream_drains_pending_completion_before_create`
    in `crates/storage/src/cluster/local/tests/multipart.rs`.
  - `S6-UPC3`: `non_current_epoch_unix_upload_part_stream_session_create_fails_closed_without_remote_mutation`,
    `control_plane_peering_unix_upload_part_session_old_primary_fails_closed_without_remote_mutation`
    in `crates/storage/src/cluster/local/tests/unix_clients.rs`.
  - `S6-READ1`: `get_object_epoch_change_after_read_snapshot_uses_pinned_route`,
    `head_object_epoch_change_after_read_snapshot_uses_pinned_route`,
    and `get_uses_retained_payload_route_over_unix_after_data_pg_move_and_metadata_reads_stay_available`
    in `crates/server-core/src/coordinator/core_tests.rs`.
  - `S6-READ2`: `cross_epoch_segment_read_uses_retained_route_over_unix_storage_nodes`
    in `crates/storage/src/cluster/local/tests/unix_clients.rs`.
  - `S6-READ3`: `read_and_list_fail_closed_while_object_metadata_pg_is_peering`
    in `crates/server-core/src/coordinator/core_tests.rs` and
    `object_read_snapshot_fails_closed_while_metadata_pg_is_peering`
    in `crates/storage/src/cluster/local/tests/object_read.rs`.
  - `S6-LIST1`: `list_objects_epoch_change_before_storage_list_uses_pinned_route`,
    `list_objects_continuation_survives_epoch_change_between_pages`,
    `list_objects_delimiter_continuation_survives_epoch_change_between_pages`,
    `list_object_versions_continuation_survives_epoch_change_between_pages`, and
    `list_object_versions_delimiter_continuation_survives_epoch_change_between_pages`
    in `crates/server-core/src/coordinator/core_tests.rs`.
  - `S6-LIST2`: `composite_object_listings_fan_out_to_routed_pg_primaries` and
    `composite_bucket_listings_fail_closed_while_any_metadata_pg_is_peering`
    in `crates/storage/src/cluster/local/tests/bucket_commands.rs`.

  Harness requirements:
  - failpoints must be named and token-scoped; no sleeps or global "next operation" hooks
    that unrelated tests/background workers can consume;
  - every pause must be observable by the test before it mutates the route map or PG state;
  - positive crossing tests must assert exactly one committed metadata command and final
    user-visible state;
  - fail-closed tests must assert no visible partial state, no pending command in source or
    current epochs, no leaked bucket-write reservation/drain, and correct shard/ack cleanup
    or preservation according to the operation stage;
  - installed-Unix variants are required for boundaries where RPC command-build/apply or
    remote cleanup semantics differ from local stores; otherwise local tests are preferred
    for precision and speed.

  Exit criteria:
  1. The matrix above is either marked covered with exact test names or has a deliberate
     open item linked to another plan. Rows marked partially covered must identify the
     missing boundary or route-state shape explicitly.
  2. Ordinary object/MPU/read/list partial-state failures are pinned by deterministic cargo
     tests rather than waiting for smoke discovery.
  3. Whole-process route-change smokes remain as integration confidence only; they are not
     the sole evidence for any high-risk boundary.
  4. Bucket-delete-specific fault interleavings remain tracked in the bucket-delete
     hardening work, with only shared failpoint API requirements duplicated here.

- **Slice 7: Control-plane RPC retry and command check-applied semantics.** The current
  Unix control-plane RPC path uses a single one-second read/write timeout on both client and
  server sides. A transient pause such as a network switch reboot, scheduler stall, or
  overloaded authority worker can therefore leave the caller unable to distinguish "command
  was not applied" from "command was applied but the response was lost". Treating all such
  failures as hard failures makes live admin operations brittle; blindly retrying all
  commands is also unsafe because some commands advance route/provenance state.

  Split the work by command semantics:
  - **Read-only RPCs, including read-only runtime-map fetches:** retry/reconnect with
    bounded backoff and an overall deadline. These do not need check-applied logic.
    **Status:** the Unix `RuntimeMapSnapshot` client path now reconnects/retries
    retryable transport failures within a bounded deadline, with regression coverage for a
    lost response after request submission. Do not classify an RPC as read-only just
    because it returns a runtime map:
    `FencePgForMetadataTransferRuntimeMap` and
    `SetPgActingSetWithMetadataTransferRuntimeMap` mutate control-plane state before
    returning their map and therefore belong with the command-specific check-applied paths
    below.
  - **Convergent set-style commands:** `SetNodeMembership`, `MarkNodeAvailability`,
    `SetPgState`, and probably `SetPgActingSet` can be retried only after confirming the
    apply path is idempotent/no-op when the target value is already current. For
    `SetPgActingSet`, prefer checking the runtime map for the expected acting set before
    resubmitting, because even logically identical route updates can create noisy extra
    epochs if the apply path is not strictly idempotent. **Status:** the live Unix
    `SetPgActingSet` path now uses a checked helper that treats response loss after
    submission as uncertain, observes the runtime map for the target acting set, and fails
    closed if a current different route is visible. Regressions cover both the already-visible
    success case and the stale retry case where another admin transition wins after the lost
    response.
  - **Time-based liveness commands:** `RecordNodeHeartbeat` and `ExpireHeartbeatLeases`
    need bounded retry/reconnect, but with care that retrying the same timestamp/deadline is
    monotonic and cannot shorten a valid lease or resurrect an expired one. **Status:**
    the Unix `RefreshNodeHeartbeat` path now retries retryable transport failures within a
    bounded deadline capped by the requested lease duration. Repeating a heartbeat inside
    that lease window is monotonic for a live node: it refreshes the same node
    incarnation/endpoint and can extend, but not shorten, the lease. Storage regressions
    inject response loss after the first heartbeat apply and verify both in-window retry
    returning a later lease deadline and short-lease fail-closed behavior before the generic
    control-plane retry deadline could resurrect an expired lease.
  - **Non-idempotent route/provenance transitions:** add explicit check-applied paths for
    `FencePgForMetadataTransfer`, `FencePgForMetadataTransferRuntimeMap`,
    `SetPgActingSetWithMetadataTransfer`,
    `SetPgActingSetWithMetadataTransferRuntimeMap`, `CompletePgPeering`, and
    `CompleteReadyPgPeerings`. On timeout, broken pipe, connection reset, or authority
    disconnect after submit, re-read the current runtime map and decide whether the intended
    state is already present:
    - `FencePgForMetadataTransfer`: PG is already fenced/peering with the expected source
      route, source lease, and transfer proof. **Status:** the live Unix
      metadata-transfer commands no longer use the bare epoch-only
      `FencePgForMetadataTransfer` RPC; the standalone live fence path and the full transfer
      flow both use checked `FencePgForMetadataTransferRuntimeMap` helpers. The runtime map
      exposes the peering route but not the private fence bit, so the helper relies on the
      audited convergent apply path: after a fence applies, resubmitting the same fence
      returns the same epoch/source lease without advancing state. A storage regression
      injects response loss after the first fence apply and asserts the retry is the same
      fence command and does not bump the epoch again. The bare Unix epoch-only helper
      remains a low-level RPC wrapper and should not be used by live admin flows that need
      response-loss confirmation.
    - `SetPgActingSetWithMetadataTransfer`: PG route has the expected acting set and matching
      metadata-transfer proof. **Status:** the Unix `SetPgActingSetWithMetadataTransfer`
      and `SetPgActingSetWithMetadataTransferRuntimeMap` live paths now treat read-side
      response loss after request submission as uncertain, then read the runtime map until
      they observe the expected peering route, acting set, and metadata-transfer proof.
      Storage regressions inject response loss after the authority applies each RPC form.
    - `CompletePgPeering` / `CompleteReadyPgPeerings`: PG is active with the expected
      primary, node incarnation, and route/proof state.

  Do not hide these distinctions behind a generic "retry every admin command" wrapper.
  The retry layer should classify transport failures as transient, but mutating admin flows
  must provide command-specific observation predicates that prove whether the lost-response
  command took effect. Tests should inject a response-loss failure after the authority has
  applied each non-idempotent command, then assert the live admin path observes the applied
  state and does not submit a second incompatible transition.
