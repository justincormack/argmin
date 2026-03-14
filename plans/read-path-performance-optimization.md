# Read Path Performance Optimization

## Context

After fixing full-object buffering and moving large reads onto `ReadHandle`, large
downloads are now memory-bounded as intended, but throughput regressed materially.
Empirically, large local reads are roughly an order of magnitude slower than the
older fully-buffered path, while CPU and system load remain fairly low.

This plan captures what we measured, what we ruled out, and the recommended order
for optimization work.

## What We Measured

### 1. HTTP chunk-size sweep

We added `ARGMIN_STREAM_READ_CHUNK_SIZE` and benchmarked cached local `GET`
throughput on a `2 GiB` multipart object.

Results:

- `1 MiB`: `1.253s`
- `4 MiB`: `1.831s`
- `8 MiB`: `2.120s`

Conclusion:

- larger HTTP chunk sizes were worse
- HTTP chunk size is not the main bottleneck
- keep the default at `1 MiB`

### 2. Multipart part-size sweep

We uploaded the same `2 GiB` object with different multipart part sizes and measured
`GET` throughput:

- `8 MiB` parts:
  - `1.392s`
  - `1.377s`
  - `1.386s`
- `64 MiB` parts:
  - `1.377s`
  - `1.387s`
  - `1.375s`
- `128 MiB` parts:
  - `1.387s`
  - `1.379s`
  - `1.372s`

Conclusion:

- larger multipart parts did not materially improve throughput
- small multipart part size is not the primary explanation for the current
  regression in this benchmark

### 3. CPU profile (`perf`)

Large local `GET` profiling showed:

- `read_shard` / `std::fs::read::inner` dominate the hot path
- `memmove` is also very hot
- socket `writev` is significant
- EC CPU itself is not the dominant bottleneck

Representative results:

- `20.79%` in
  `<storage::pg_store::PgStore as storage::traits::ShardStore>::read_shard`
- `20.62%` in `std::fs::read::inner`
- `44.69%` in `__memmove_avx512_unaligned_erms`

Interpretation:

- we are paying heavily for repeated shard reads and copy/assembly work
- the bottleneck is not primarily checksum calculation or SQLite lookup

### 4. Syscall profiling (`strace`)

Large local `GET` syscall tracing showed:

- many `openat` / `read` / `close` operations
- many `futex` waits, but those reflect whole-process blocked time and should not
  be treated as proof that mutex contention is the root bottleneck

This is consistent with the CPU profile: there is substantial churn around
repeated shard-file access.

### 5. Read amplification

We traced a `2 GiB` object `GET` and summed only shard-file read syscalls under
`.../pg-*/shards/...`.

Results:

- object bytes returned: `2,147,483,648`
- shard bytes read: `3,221,225,472`
- read amplification: `1.500000x`

Conclusion:

- low-level shard reads are not over-reading beyond the expected `4+2` EC overhead
- the main issue is not excess read amplification at the storage syscall level

## Current Read-Shard Shape

`PgStore::read_shard()` currently does:

1. SQLite metadata lookup
2. `fs::read(shard_path)` into a fresh `Vec<u8>`
3. CRC64 over that full buffer
4. return `ShardData { data: Vec<u8>, ... }`

This shape strongly suggests two avoidable costs:

- repeated whole-file allocation and read into fresh buffers
- repeated copy/assembly of returned shard buffers into output chunks

## What We Ruled Out

These are not the first place to optimize:

1. HTTP response chunk size
2. Multipart part size
3. SQLite lookup inside `read_shard`
4. CRC64 itself as the main hot spot
5. Storage-level over-read beyond EC overhead

## Recommended Optimization Direction

### Goal

Reduce repeated shard-read allocation/copy overhead without regressing correctness,
streaming semantics, or the new retained-payload lifetime model.

### Stage 1: Stop using `fs::read()` on the hot path

Replace the current whole-file read API with a caller-buffer or reader-based API.

Recommended direction:

- add a ranged read interface such as `read_shard_into(...)`
- or add a shard-reader object that keeps an fd open and supports sequential reads

Properties:

- caller provides reusable buffers
- shard reads no longer allocate a fresh `Vec` per call
- read only the needed window for the current output chunk

This is the highest-value first optimization.

### Stage 2: Reuse shard file descriptors within a reader lifetime

Current traces suggest repeated open/read/close churn.

Recommended direction:

- keep shard files open within `ShardSetReader`, `PartShardReader`, and related
  reader variants for the duration of the read window or request
- use `pread` / `read` into caller-owned buffers instead of reopening files

This should reduce syscall churn and kernel-side path work.

### Stage 3: Reduce assembly copies

Once shard data is read into reusable buffers, reduce extra movement on the
reconstruction path.

Recommended direction:

- reconstruct directly into the final output chunk buffer where possible
- avoid intermediate `Vec` creation for assembled chunks
- consider a segmented internal chunk representation only if measurement shows it
  will reduce copying materially

Important:

- true zero-copy is not realistic for the normal EC path, because bytes must be
  reconstructed into response order
- the target is "one output buffer", not "no copies at all"

### Stage 4: Re-measure before more ambitious design changes

After Stages 1-3, rerun:

- large local `GET` throughput benchmarks
- `perf`
- shard-read syscall trace

Only then decide whether more invasive work is justified, such as:

- segmented output chunks
- larger internal readahead windows
- alternative checksum placement

## Implementation Order

1. Introduce a non-allocating shard-read API in `storage`
2. Switch `ShardSetReader` to it
3. Switch multipart/chunk-manifest readers to it
4. Add fd reuse inside reader lifetimes
5. Refactor assembly to write directly into output buffers
6. Re-profile and decide whether segmented output is worthwhile

## Non-Goals For This Pass

1. Changing HTTP chunk framing semantics
2. Changing EC layout
3. Removing shard CRC verification
4. General mutex/lock tuning without evidence
5. Full observability work; that should be planned separately

## Success Criteria

We should consider this optimization successful if:

1. large local `GET` throughput materially improves
2. `perf` shows less time in `std::fs::read::inner`
3. `memmove` share drops materially
4. syscall counts for `openat` / `read` / `close` on shard files fall
5. memory remains bounded under large streaming reads
