<!-- Copyright The Argmin Authors. -->
<!-- SPDX-License-Identifier: CC-BY-4.0 -->

# Durable Cleanup Ownership — Unresolved Findings

Status: regression evidence only. The unfinished fix has been removed; no new
ownership tables, worker, staging tickets, RPC operations, or format versions are
part of this slice. A shared fix will be designed and reviewed separately under
[unified-staged-write-publication-plan.md](unified-staged-write-publication-plan.md#shared-cleanup-investigation-before-further-implementation).

## Lost Direct-PUT Reservation Cleanup

The [discovery experiment](bounded-model-checking-plan.md#discovery-experiment--cleanup-without-a-model-supplied-retry)
found that cancellation can lose the obligation to release a generation reservation:

1. Two admitted direct PUTs reserve generations and stage payload.
2. One leaves an exact pending command with a retained authorized-recovery flight.
3. The other drops or explicitly discards its payload handle. Shard cleanup runs,
   but reservation release is blocked by the unrelated recovery flight.
4. Production maintenance recovers the pending command, then performs healthy
   stream, shard-audit, and reclaim passes.
5. The cancelled caller's reservation remains. No durable cleanup obligation was
   installed for maintenance to discover.

The permanent test
`production_maintenance_releases_cancelled_direct_put_reservation_after_recovery`
covers both command owners and both cancellation forms. It requires cleanup
without a model-supplied release retry. The non-ignored paired control,
`production_maintenance_preserves_live_direct_put_and_cleans_cancellation_after_recovery`,
checks live-request preservation and cancellation after the pending slot is free.

This establishes retained unused metadata, not object-data loss. An in-memory
retry queue alone would not establish recovery after restart; an age-only scan
cannot distinguish abandoned reservations from slow live callers.

## Late Writes After Cleanup

A separate schedule shows that completing metadata cleanup does not fence physical
writes made under a still-valid route:

1. Prepare a direct PUT or stream segment and retain its original route admission.
2. Release the direct-PUT generation reservation, or finish stream-session abort.
3. Deliver the prepared physical write without a surviving frontend running
   compensating cleanup.

The direct regression `direct_put_release_fences_delayed_staging` uses the
existing explicit reservation-release operation and verifies release on every
replica. It also checks every expected shard file and acknowledgement before and
after the delayed write, independently of its result and before any returned
payload handle is dropped. It does not depend on the removed prototype's
generation scanner.

Four stream regressions cover PutObject and UploadPart sessions. For each target,
one pauses after preparation; the other first writes and publishes the segment,
checks its exact files and durable acknowledgement rows, aborts and verifies
deletion, then delivers a delayed duplicate. The latter is not the unrecorded-
manifest crash window. Both targets previously reproduced successful late writes
leaving all three 2+1 shard files present without acknowledgement rows or a session.

These tests split local production phases deterministically, without sleeps. They
do not establish transport-gap, node-restart, or whole-MPU-abort coverage.
The existing caller-compensation tests remain useful but do not prove safety
after that caller disappears.

## Retained Regressions

Owner-local tests live in:

- `crates/storage/src/cluster/local/tests/direct_put_cleanup_progress.rs`
- `crates/storage/src/cluster/local/tests/late_stream_cleanup.rs`

The six known-failing regressions have explicit `#[ignore = "..."]` reasons
pointing here. The healthy control remains enabled. Ignoring the reproducers
records unresolved bugs; it does not weaken their assertions or claim a fix.

Run the normal selection, then explicitly reproduce the unresolved failures:

```bash
cargo nextest run -p storage -E 'test(cleanup_progress) | test(late_stream_cleanup)'
cargo nextest run -p storage -E 'test(cleanup_progress) | test(late_stream_cleanup)' --run-ignored ignored-only --no-fail-fast
```

## Shared Fix Requirements

Shard and metadata publication are not transactional. The known crash window
between writing shards and recording their manifest still needs orphan scanning;
this investigation does not require a metadata command per shard or claim to
eliminate that window.

A future fix must use a common physical write/cleanup exclusion contract across
direct PUT, streamed PUT, and UploadPart. Durable provenance can differ, but
cleanup must retain its obligation through failed release/deletion, unavailable
nodes, restart, and retained-route changes. Pending and committed payload must
remain protected, while cleanup cannot declare completion while delayed writes
can recreate it. Scanner candidates are hints, not authority to revoke live state.

The removed metadata-owner and node-ticket prototypes were incomplete design
experiments, not selected architecture. Reconcile any future lease, cutoff,
ticket, or scanner design in the unification plan before production integration.
Measure changes to small-PUT metadata cost and follow the versioning guide for
any eventual durable or wire-format changes. Fix-only tests for the removed
prototype are not retained as evidence for existing production behavior.
