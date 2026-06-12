# Production Backpressure Plan

Status: draft

## Context

Phase 10.9 of the multihost transition is finding real problems: unbounded
request work, retry storms, lossy storage RPC overload, and expected contention
escaping as the wrong public failure shape. Those are correctness-adjacent and
should be fixed before continuing into failure, peering, repair, and migration.

That does not mean the current UAT soak workload is a good production
backpressure model. The most aggressive soak shape runs multiple storage
processes on one host, drives the service from a local client, and concentrates
fsync and metadata pressure onto the same physical disks. It is valuable because
it amplifies request-boundary bugs, but it should not be used alone to tune
production throughput policy.

This plan owns the broader production backpressure work that should happen after
the Phase 10.9 stabilization gate and after a more representative harness
exists.

## Goals

1. define capacity policy from realistic production-style workloads, not only
   from single-host stress tests
2. preserve S3 correctness and non-ambiguous write outcomes under overload
3. return S3-shaped `SlowDown` early enough for clients to pace load
4. keep foreground reads and bounded writes progressing while background work is
   saturated
5. avoid tuning decisions that merely optimize the test harness at the expense
   of real deployments

## Non-Goals

1. Do not relax AWS-visible S3 behavior.
2. Do not use client retries to hide server-side ambiguous commits, EOFs, or
   HTTP 500s.
3. Do not make adaptive admission a Phase 10.9 exit requirement.
4. Do not infer production SLOs from the local six-process UAT soak alone.

## Prerequisites

1. Phase 10.9 has bounded request-path work and typed public failure shapes for
   expected contention and overload.
2. Storage RPC/session saturation no longer appears as public EOF or SDK
   operation-attempt timeout.
3. Basic fixed admission classes and metrics exist for request admission,
   storage RPC admission, metadata-command recovery, and known hot paths.
4. A production-style harness exists with:
   - separate frontend and storage-node processes
   - storage nodes on separate disks or separate IO scheduling domains
   - real interprocess or network transport
   - configurable client distance and bandwidth/latency
   - mixed workloads rather than only full-suite test concurrency
   - controlled restarts, route changes, and failure injection for correctness
     runs

## Work Items

1. workload model
   - define representative foreground mixes for GET/HEAD, direct PUT, stream
     PUT, MPU upload/complete, CopyObject/UploadPartCopy, list/version-list, and
     control-plane operations
   - define background mixes for lifecycle, reclaim, delete finalization,
     multipart cleanup, repair, scrub, and migration
   - record expected object sizes, concurrency ranges, hot-key/hot-bucket
     shapes, read/write ratios, and tenant/account skew
2. production-style harness
   - add repeatable profiles for normal, overload, hot-PG, hot-bucket,
     large-object, list-heavy, and background-saturated workloads
   - keep the existing single-host UAT soak as an overload amplifier, but label
     it separately from production workload evidence
   - emit per-profile summaries for throughput, p50/p95/p99 latency,
     `SlowDown`, `OperationAborted`, HTTP 500, EOF/transport failures, queue
     waits, bytes in flight, and background-vs-foreground capacity use
3. capacity resources and fixed policy
   - promote the fixed admission classes from Phase 10.9 into a shared capacity
     lease abstraction where useful
   - model frontend requests, streaming body buffers, internal copy bytes,
     per-node RPC sessions, shard reads, shard writes, bytes in flight,
     per-PG metadata mutation starts, metadata-command apply/recovery, and
     background worker leases
   - keep limits static until the harness can show which resource is actually
     saturated
4. side-effect-aware overload
   - before durable command/session/reservation identity exists, capacity
     failure may return `SlowDown` only if no durable side effect was created
   - after staging starts but before publish, return `SlowDown` only after the
     normal cleanup path proves no visible mutation committed
   - after publish/commit is known durable, return the committed result
   - if publish/commit outcome is unknown, fail closed with diagnostics rather
     than returning retryable overload
5. operation cost model
   - charge metadata-only operations for metadata/RPC units
   - charge reads for read RPCs, read handles, and response bytes
   - charge direct PUT, stream PUT, UploadPart, MPU complete, CopyObject, and
     UploadPartCopy for mutating starts, per-segment shard reads/writes, and
     bytes in flight
   - acquire capacity for unknown-size streaming writes one segment at a time
     before reading that segment from the client
6. foreground/background separation
   - reserve foreground capacity for PUT/GET/HEAD/list/control-plane requests
   - make lifecycle, reclaim, delete finalization, scavenger, repair, scrub, and
     migration use low-priority leases with backoff
   - ensure background work keeps durable rows as the source of truth when it
     cannot acquire capacity
7. read/list separation
   - keep cheap read work (`GET`, `HEAD`, shard read, read-handle acquire)
     separate from expensive list/page/scan work
   - make list-heavy workloads feel overload before ordinary reads lose their
     low-latency reservation
   - ensure internal cleanup enumeration needed to finish already-admitted work
     does not compete with user list traffic as if it were new bulk work
8. completion pressure
   - reserve enough capacity for already-admitted work to finish or clean up
   - shift additional capacity toward completion/progress work when unfinished
     stream sessions, pending appends, pending metadata commands, staged
     payloads, or cleanup backlog grow
   - keep a configured nonzero floor for new starts
9. adaptive controller
   - add adaptive admission only after fixed-policy profiles are stable
   - adjust within configured min/max limits using completion rate, p95/p99 wait
     time, timeout count, queue depth, `SlowDown`, and transport-error signals
   - increase slowly while wait times are below target and no overload is
     emitted
   - decrease immediately when waits exceed budget, storage-node resource
     exhaustion appears, or SDK operation-attempt timeout risk is detected
   - gate the controller behind configuration until production-style harness
     results show it improves stability without hiding correctness failures
10. client pacing
   - return `SlowDown` early enough for SDK retries to pace clients
   - preserve `Retry-After`, with later tuning from overload debt once measured
   - ensure pacing does not depend on retrying transport EOFs or HTTP 500s

## Required Tests

1. production-style normal workload meets configured latency and error targets
   without `SlowDown`
2. overload profiles return bounded `SlowDown` and recover when load drops
3. large-object PUT, MPU, CopyObject, and UploadPartCopy do not starve bounded
   GET/HEAD traffic
4. list-heavy workloads do not starve cheap reads
5. background lifecycle/reclaim/repair/scrub load does not starve foreground
   PUT/GET/MPU workloads
6. unknown-size streaming writes acquire per-segment capacity before reading
   body bytes
7. staged writes that hit capacity failure clean up before returning `SlowDown`
8. unknown publish outcomes fail closed and never return retryable overload
9. adaptive admission remains inside configured min/max limits and improves
   stability compared with fixed policy on the same harness profile
10. the single-host six-process UAT soak remains useful as an overload
    amplifier, but failures from that profile are not treated as production
    throughput evidence without confirmation from production-style profiles

## Exit Criteria

1. fixed production-style workload profiles are repeatable and produce stable
   metrics
2. foreground/background and read/list separation are validated under forced
   pressure
3. side-effect-aware overload tests prove no duplicate mutation, leaked
   reservation, leaked read handle, or orphaned visible payload
4. adaptive admission is either justified by harness data and enabled behind a
   conservative default, or deliberately deferred with fixed limits retained
5. documentation distinguishes overload-amplifier UAT results from
   production-style capacity evidence
