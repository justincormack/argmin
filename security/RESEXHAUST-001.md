---
id: RESEXHAUST-001
bug_class: resource-exhaustion
title: Unbounded Vec::with_capacity from attacker-controlled count in storage-node runtime config decode
location: crates/storage/src/storage_node_server.rs:1017
function: decode_control_plane_runtime_config
confidence: High
worker: worker-7
fp_verdict: TRUE_POSITIVE
fp_rationale: "The eager allocation defect was real and corrupted durable input could abort the process, but the original attacker framing was overstated: the runtime config is inside a mode-0700 service data directory and is decoded during startup/bind, not normal refresh. This is low-severity fail-closed durable-state hardening rather than a new privilege escalation."
severity: LOW
attack_vector: Local
exploitability: Requires service-state write access
severity_rationale: "Malformed service-owned durable state could amplify a tiny file into an OOM or capacity-overflow process abort during startup. The data directory is private, and an actor able to modify it can already deny service by deleting or corrupting state, so the security impact is limited to robust bounded parsing."
status: fixed
---

## Description
`decode_control_plane_runtime_config` parses the storage-node runtime config
(text format, magic `argmin-storage-node-runtime-config-v2`) read from a file
inside `ARGMIN_DATA_DIR`. Two element counts — `pg_id_count` and
`recovery_count` — are parsed as an unbounded `usize` via `parse_labeled_usize`
(which is a plain `str::parse::<usize>()`), and each is passed directly to
`Vec::with_capacity(count)` **before** any of the corresponding elements are
read from the input.

Unlike every binary RPC decoder in this codebase (which bounds counts against
the remaining payload via `read_collection_len` / `read_count_with_limit`),
this text parser performs no `count <= remaining_items` check. A config line
such as `pg_ids 4000000000` causes an eager `Vec::<u32>::with_capacity(4e9)`
(~16 GB) even though the file is only a few dozen bytes. `recovery_count`
elements are ~48-byte tuples, so `pending_metadata_command_recoveries 4000000000`
requests ~192 GB. `Vec::with_capacity` either aborts the process on allocation
failure (OOM) or, for values large enough that `count * size_of::<T>()` exceeds
`isize::MAX`, panics with "capacity overflow" — both terminate the storage-node
process (availability loss).

## Code
```rust
let pg_id_count = parse_labeled_usize(path, lines.next(), "pg_ids")?;
let mut pg_ids = Vec::with_capacity(pg_id_count);   // eager, unbounded alloc
for _ in 0..pg_id_count {
    let line = lines
        .next()
        .ok_or_else(|| runtime_config_invalid(path, "missing PG id"))?;
    pg_ids.push(parse_u32_field(path, line, "PG id")?);
}
// ...
let recovery_count =
    parse_labeled_usize(path, lines.next(), "pending_metadata_command_recoveries")?;
let mut pending_metadata_command_recoveries = Vec::with_capacity(recovery_count); // eager, unbounded
```

`parse_labeled_usize` -> `parse_usize_field` is an unbounded decimal `usize`
parse (no clamp to a protocol maximum).

## Data flow
- **Source:** runtime config file at `control_plane_runtime_config_path(data_dir)` under `ARGMIN_DATA_DIR`, read by `load_control_plane_runtime_config` via `fs::read_to_string` (crates/storage/src/storage_node_server.rs:670).
- **Sink:** `Vec::with_capacity(pg_id_count)` (line 1017) and `Vec::with_capacity(recovery_count)` (line 1029).
- **Validation:** none — the count is used for `with_capacity` before the loop that would fail on missing element lines; no `min(count, MAX)` / bound-against-remaining-lines check.

## Reachability trace
`StorageNodeProcessConfig::load_control_plane_runtime_config` (reads file) →
`decode_control_plane_runtime_config` → `parse_labeled_usize("pg_ids")` →
`Vec::with_capacity(pg_id_count)`.

## Impact
A LOCAL_UNPRIVILEGED attacker able to write the runtime config file in the data
directory (the data dir is an explicit LOCAL trust boundary in this review;
TOCTOU / symlink / world-writable concerns are in scope) can cause the
storage-node process to allocate tens of gigabytes and abort/OOM on startup or
config refresh — a persistent denial of service that prevents the node from
coming up.

## Mitigations checked
- No `// SAFETY` relevance (safe code).
- No count cap: contrast `read_collection_len` (crates/storage/src/control_plane.rs:17500) and `read_count_with_limit` in storage_rpc.rs, which bound `count <= remaining/min_item_len` before allocating. This text decoder has no equivalent.
- `overflow-checks` unset (release wraps), but this is an allocation DoS, not an arithmetic panic — `with_capacity` itself aborts/panics.
- The follow-on `for` loop would error on a missing line, but only *after* the eager `with_capacity` has already been requested.

## Recommendation
Bound each count before allocating: either clamp with a documented protocol
maximum (as the binary decoders do) or avoid eager reservation entirely by
using `Vec::new()` and letting the per-element `push` grow incrementally, or
validate `count` against the number of remaining lines. Apply the same fix to
`recovery_count` on line 1029.

## Validity assessment

The allocation defect is a true positive. Both `pg_ids` and
`pending_metadata_command_recoveries` accepted an arbitrary `usize` count and
reserved the corresponding vector before proving that the file contained that
many records. A short corrupt file could therefore request an allocation far
larger than its physical representation.

The original security characterization was too strong. The file is stored in
a service data directory whose mode is enforced as `0700`; an ordinary local
unprivileged account cannot replace it. A process with the service identity or
equivalent filesystem access can already prevent startup by deleting or
corrupting durable state. The decoder is reached during storage-node startup
and bind validation, while normal control-plane refresh installs an in-memory
typed configuration and persists it without decoding this file. The issue is
therefore retained as low-severity local durable-state hardening rather than a
distinct privilege escalation.

The original recommendation was also incomplete. Removing the eager vector
reservation alone would leave the preceding whole-file `read_to_string`
unbounded and would not address special-file blocking, symlink traversal, or
writing a configuration larger than the loader accepts.

## Resolution

Fixed in
[`89c2c388cee08a7857878253d3bcd2584439fbc8`](https://github.com/justincormack/argmin/commit/89c2c388cee08a7857878253d3bcd2584439fbc8)
(`Harden storage runtime config decoding`).

The runtime-config loader now opens the file with `O_NOFOLLOW`, `O_CLOEXEC`,
and `O_NONBLOCK`, verifies the opened descriptor is a regular file, and reads
at most eight times the bounded control-plane frame size plus one detection
byte. It rejects oversized files both from descriptor metadata and while
reading, so file growth cannot race the initial size check. Persistence
enforces the same byte limit and cannot write state the loader would reject.

The decoder validates every outer collection count against the remaining
bounded records and grows `pg_ids` and pending-recovery vectors only as valid
records are parsed. Fixed-width recovery records are parsed field by field
without an attacker-sized temporary field vector.

Persistence was hardened at the same boundary. A stale regular staging file
is unlinked before an atomically exclusive `0600` staging inode is created
with `create_new`, `O_NOFOLLOW`, and `O_CLOEXEC`; symlinks and special files
are rejected without touching their targets.

The deterministic regressions cover oversized counts for all runtime-config
sections, oversized sparse files, FIFO rejection without blocking, non-regular
input, read-path and staging-path symlinks, stale regular staging residue, and
canonical round-trip loading. The fixing commit passed the complete workspace
nextest suite (7,628 tests) and Clippy across all targets and features with
warnings denied.
