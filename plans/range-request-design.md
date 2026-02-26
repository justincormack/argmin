# Optimized Shard Reading + Range Request Support

## Overview

S3-compatible HTTP Range request support and optimized shard reading. With
systematic Reed-Solomon, data shards contain original bytes verbatim, so we
can read only the shards covering the needed byte range instead of all k+m.

## Schema

- `size` — user data size (existing, used for S3 Content-Length / `<Size>`)
- `total_size` — metadata blob + user data, before EC padding (new)
- `metadata_size = total_size - size` (computed, not stored)

Objects written before this change have `total_size = 0` and fall back to the
legacy read-all-shards path.

## Layered design

### Low-level: `read_range(start, end)`

Maps byte offsets in the stored blob (metadata + user data) to shard indices,
reads those shards, extracts the requested byte range.

### Mid-level wrappers

- **HEAD** → `read_range(0, metadata_size - 1)` — reads only metadata shards
- **Range GET** → two `read_range` calls: one for metadata, one for user data range
- **Full GET** → `read_range(0, total_size - 1)`, split at metadata_size boundary

## Shard planning math

```
padded_total = ceil(total_size, k)
shard_size   = padded_total / k

first_shard = start / shard_size
last_shard  = min(end / shard_size, k - 1)
```

## Integrity: whole-shard reads are mandatory

**Important:** Each shard has a CRC64-NVME checksum verified on read by
`read_shard()`. There are no sub-shard checksums. This means `read_range`
must always read *whole* shards even when only a subset of bytes is needed —
it then discards bytes outside the requested range after verification.

For range requests that don't align to shard boundaries, this means we read
more data than strictly necessary and discard the excess. This is unavoidable
without adding finer-grained checksums (e.g. per-block checksums within
shards). The overhead is bounded: at most `shard_size - 1` extra bytes at
each end of the range, which for typical configurations is small relative to
the total transfer.

If sub-shard checksums are added in the future, `read_range` could be updated
to read partial shards, but the current whole-shard approach is the only safe
option.

## HTTP layer

- `Accept-Ranges: bytes` on GET and HEAD responses
- Range header parsed → `ByteRange::parse()` (single-range only, per S3 spec)
- Satisfiable range → 206 with `Content-Range: bytes start-end/total`
- Unsatisfiable range → 416 with `Content-Range: bytes */total`

## Edge cases

- **Zero-length objects**: `total_size = metadata_size` (>=7), HEAD works, Range returns 416
- **Legacy objects** (`total_size = 0`): fall back to read-all-data-shards path
- **Range clamping**: `bytes=0-999999` on 100-byte object → clamp end to 99, return 206
- **Suffix exceeds size**: `bytes=-999999` on 100-byte object → return full object as 206
- **Missing shards**: `read_data_shards` handles EC reconstruction transparently

## Files modified

| File | Change |
|------|--------|
| `crates/storage/src/types.rs` | `total_size: u64` on ObjectRecord + PutObjectMetaReq |
| `crates/storage/src/schema.rs` | `total_size` column (DEFAULT 0 for compat) |
| `crates/storage/src/pg_store.rs` | INSERT/SELECT queries updated |
| `crates/storage/src/memory_store.rs` | total_size in record construction |
| `crates/debug-cli/src/main.rs` | TOTAL_SIZE column in objects display |
| `crates/server/src/coordinator.rs` | `read_range`, shard planning, get/head/range ops |
| `crates/server/src/range.rs` | ByteRange parse/resolve |
| `crates/server/src/error.rs` | InvalidRange variant |
| `crates/server/src/http/response.rs` | Accept-Ranges, 206/416 responses |
| `crates/server/src/http/mod.rs` | Range header dispatch |
