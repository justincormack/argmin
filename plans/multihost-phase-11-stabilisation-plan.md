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

1. **Define recovery semantics explicitly.** Document, in
   [`guides/storage-cluster-invariants.md`](../guides/storage-cluster-invariants.md), that
   opening a PG store is a recovery boundary and state which anomaly classes fail closed
   (possible corruption: hash-chain break, digest mismatch, forked log) versus which are
   reconciled (benign crash leftovers: terminal pending slot, epoch-mismatched orphan
   slot). The heartbeat path currently reconciles silently; recovery should make this
   distinction deliberately.

2. **Recovery is a method on an opened store, invoked by node-identity-owning callers.**
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

2b. **Cached-table refresh and per-table diagnostics on top of replay validation.** Replay
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
   Decide explicitly whether xor-cancelled per-table drift should fail closed (it indicates a
   trigger bug — the Slice 2 root cause — and silent refresh could mask it) or warn-and-repair
   (the materialised state is provably correct, so serving can continue). The recommendation
   is warn-and-repair for Slice 1 with a prominent trace, and let Slice 2's trigger-body check
   escalate to fail-closed once the root cause is detectable. Either way, refresh runs only
   after detection so the diagnostic is meaningful.

3. **Generalise the existing commit-fault hook.** The `fail_next_delete_finalized_bucket_commit`
   flag (`pg_store.rs:387`) is a per-mutator test hook. Lift it to a generic
   `fail_next_metadata_txn_commit` hook so the crash-recovery property test (work item 6)
   can inject a commit failure into any digest-affecting transaction, not just finalized
   bucket delete.

   Progress update: the generic hook now exists on `PgStore` and is consumed by
   `with_immediate_txn` immediately before `COMMIT`. It rolls back, invalidates the clean
   digest revision, and returns the caller's normal commit-context error. The old
   delete-finalized hook still exists for its hand-written transaction path; the generic hook
   covers the common metadata transaction helper used by most digest-affecting mutators.

4. **Make pending-slot removal transactional at the safe finalization boundary.** A first
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

5. **Consolidate the divergent open-time cleanups.** Once `PgStore::recover` reconciles
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

6. **Crash-recovery property test for every digest-affecting mutator.** Using the
   generalised hook from work item 3, for each metadata-mutating transaction: inject a
   commit failure, reopen the PG through `PgStore::open` then `pg.recover(ctx)`, and assert
   (a) per-table materialised digests match the cached `metadata_table_digests` rows and
       `metadata_command_replica_state.state_digest` (not just cached-vs-cached),
   (b) no orphaned pending slots remain,
   (c) the command-log hash chain is intact,
   (d) `metadata_command_replica_state` matches materialised state,
   (e) the PG can accept the next command without a spurious retry/drain cycle.
   This single harness would have caught `c828f3a5`, `c5776092`, and `791b409c`.

7. **Storage-node restart coverage.** Add a test that opens a PG through
   `StorageNodeServer::bind` (the production restart path), after a crash that orphans a
   terminal slot and a commit-failed transaction, and asserts the server does not serve a
   read until recovery has reconciled the state. This pins the path that currently has zero
   recovery.

   Progress update: `StorageNodeServer::bind` now has direct regressions for cleaning a
   terminal pending metadata command during bind recovery and for failing closed on a
   corrupted metadata state digest before serving.

### Exit criteria

1. `PgStore::recover(&self, PgStoreRecoveryContext { node_id })` reconciles
   epoch-mismatched orphan and terminal pending slots (in that order) and fails closed on
   materialised-digest or hash-chain mismatch. `PgStore::open` stays raw; recovery is
   invoked by node-identity-owning callers, not threaded through `SharedStorageNode::open`.
2. `StorageNodeServer::bind` and the local-cluster builder both call `recover` per PG with
   their node id, so a restarted storage node never serves on an unvalidated PG.
3. Pending-slot finalization is transactional and exact-match, while
   `record_metadata_command_applied` remains per-replica recording only so partial-fanout
   recovery remains correct.
4. The local-cluster build path and the storage-node server share one recovery code path;
   `clean_terminal_primary_pending_slot_on_open` is removed.
5. A crash-recovery property test covers every digest-affecting mutator and asserts the
   five post-recovery invariants above, including materialised-vs-cached digest agreement
   and that an epoch-mismatched orphan slot does not block recovery.
6. The reactive heartbeat cleanups (`c5776092`, `791b409c`) remain as a defence-in-depth
   heartbeat-side check but are no longer the primary correctness mechanism; this is
   documented in `guides/storage-cluster-invariants.md`.

### Out of scope for Slice 1

- SQLite trigger SQL body verification (tracked as Slice 2). The materialised digest scan
  in work item 2b catches the *symptom* of a stale trigger definition at open; Slice 2
  catches the *root cause* and enables trusting the cached-table fast path so the
  materialised scan can be downgraded to a periodic/diagnostic check.
- Tightening the fenced-transfer source proof floor and the same-epoch digest-only
  relaxation (tracked as Slice 3).
- Control-plane open->save->reload transition property tests (tracked as Slice 4).

## Tracked future slices

These are recorded here so the backlog is visible; each will be expanded into a full slice
when prioritised. They correspond to the remaining findings from the Phase 11 review.

- **Slice 2: Trigger SQL body verification.** `metadata_digest_triggers_complete`
  (`command_log.rs:991`) only checks trigger names in `sqlite_master`. A DB created under
  an older schema passes the check with triggers that compute row digests from the old
  column set, silently corrupting every subsequent digest/proof. Store an expected hash of
  each table's trigger SQL (or a schema-generation tag) and on mismatch drop, recreate, and
  force a full digest recompute. Add a schema-evolution test that opens a store built under
  an N-versions-old trigger snapshot.

- **Slice 3: Metadata proof-floor tightening.**
  - The same-epoch digest-only relaxation
    (`metadata_proof_satisfies_active_primary_observation_floor_impl` at
    `control_plane.rs:7263`, the `allow_same_epoch_digest_only_progress` branch at `:7274`,
    gated by `297fe9bb`) masks state-only divergence between replicas that applied the
    same log. Fold the relaxation behind a narrow, named escape hatch so the audit trail
    of *why* divergence is allowed is explicit and cannot silently propagate to peering.
  - `metadata_proof_satisfies_fenced_transfer_source_floor` (`control_plane.rs:7295`)
    accepts any non-zero-hash different-digest proof when
    `active_metadata_transfer_imported` is set; tighten it to require log-index ordering.
  - Add a divergent-replica proptest: apply the same command log to two replicas, inject one
    out-of-band mutation on one, assert peering completion fails closed.

- **Slice 4: Control-plane transition open/save/reload property tests.** The
    `6f9d1605` "second restart fails" bug is the canonical shape: transition code copies
    active fields verbatim into a stricter-shaped peering target. Grep for every
    `record.peering_X = record.active_Y` assignment in `control_plane.rs` and promote the
    ad-hoc `open -> mutate -> save -> reload -> open` cycle used in the security dossiers
    to a permanent proptest over the PG state-transition graph (Active<->Peering, transfer
    marker install/clear, floor preservation, restart epoch bump).

- **Slice 5: OpenRaft spike conformance.** Run `openraft::testing::log::suite` against the
  in-memory spike log store. The plan (`multihost-transition-plan.md` around the Phase 12.1
  notes) explicitly notes this is not done; it is the canonical suite for the
  monotonicity/watermark bugs that `caf14a21`, `2c462236`, and `abf7ec65` fixed manually.

- **Slice 6: Deterministic fault-injection harness promotion.** Promote the
  token-scoped deterministic fault gate (Phase 11 work item 11) into the standard
  verification path for the high partial-state-risk boundaries: after payload shard write
  before metadata publish, after pending-command install before apply, after apply before
  response, during cleanup/finalization, and across PG state transitions. This replaces the
  slow, non-deterministic soak discovery of these bugs with fast precise tests.
