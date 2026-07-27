---
id: RESEXHAUST-002
bug_class: resource-exhaustion
title: Unbounded Vec::with_capacity from attacker-controlled route count in storage-node route decode
location: crates/storage/src/storage_node_server.rs:1083
function: decode_storage_node_routes
confidence: High
worker: worker-7
fp_verdict: TRUE_POSITIVE
fp_rationale: "The outer route-count allocation defect was real. The report understated the adjacent inner-count issue because 5 + acting_len could itself overflow before rejection. The local attacker framing was overstated because the file is private service-owned durable state, so this is low-severity bounded-parser hardening."
severity: LOW
attack_vector: Local
exploitability: Requires service-state write access
severity_rationale: "Malformed service-owned durable state could trigger excessive route-vector allocation or an overflow-checked-build panic during startup. The private data-directory boundary already gives a writer simpler denial-of-service options, limiting the security impact."
status: fixed
---

## Description
`decode_storage_node_routes` decodes the `pg_routes` / `historical_pg_routes`
sections of the storage-node runtime config file (read from `ARGMIN_DATA_DIR`).
The route `count` is parsed as an unbounded `usize` via `parse_labeled_usize`
and passed straight to `Vec::with_capacity(count)` before any route line is
read. There is no bound of `count` against the number of remaining lines or a
protocol maximum, so a tiny config file can request a multi-gigabyte eager
allocation.

`StorageNodePgRoute` is a multi-field struct, so `with_capacity(count)`
reserves `count * size_of::<StorageNodePgRoute>()` bytes. A config line such as
`pg_routes 2000000000` triggers an allocation on the order of tens of GB,
causing `Vec::with_capacity` to abort the process on OOM (or panic with
"capacity overflow" for counts where the byte size exceeds `isize::MAX`).

Note: the inner `acting_set` allocation on line 1102 is *not* affected — it is
guarded by `if fields.len() != 5 + acting_len` (line 1096), which bounds
`acting_len` to the actual number of fields on the line. Only the outer route
`count` is unbounded.

## Code
```rust
fn decode_storage_node_routes<'a>(
    path: &Path,
    lines: &mut impl Iterator<Item = &'a str>,
    label: &str,
) -> Result<Vec<StorageNodePgRoute>, StorageNodeServerError> {
    let count = parse_labeled_usize(path, lines.next(), label)?;
    let mut routes = Vec::with_capacity(count);   // eager, unbounded alloc
    for _ in 0..count {
        let line = lines
            .next()
            .ok_or_else(|| runtime_config_invalid(path, format!("missing {label} route")))?;
        // ...
    }
    Ok(routes)
}
```

## Data flow
- **Source:** runtime config file under `ARGMIN_DATA_DIR`, read by `load_control_plane_runtime_config` via `fs::read_to_string` (crates/storage/src/storage_node_server.rs:670), then `decode_control_plane_runtime_config` -> `decode_storage_node_routes`.
- **Sink:** `Vec::with_capacity(count)` at line 1083.
- **Validation:** none — `count` from `parse_labeled_usize` is an unbounded `usize` used directly for `with_capacity`.

## Reachability trace
`load_control_plane_runtime_config` (reads file) →
`decode_control_plane_runtime_config` (line 1024/1026 calls) →
`decode_storage_node_routes` → `Vec::with_capacity(count)`.

## Impact
A LOCAL_UNPRIVILEGED attacker able to write the storage-node runtime config
file can abort/OOM the storage-node process via an oversized `pg_routes` /
`historical_pg_routes` count, denying service (node fails to start or refresh
its runtime config).

## Mitigations checked
- No count cap or bound-against-remaining-input, unlike the binary RPC decoders (`read_collection_len` / `read_count_with_limit`) used everywhere else in this crate.
- The following `for` loop errors on a missing route line, but only after the eager `with_capacity` allocation has already been requested.
- Safe code; no `// SAFETY` relevance.

## Recommendation
Clamp `count` against a documented protocol maximum before allocating, or drop
the eager reservation (`Vec::new()` + incremental `push`), matching the
bounded-count discipline of the binary codecs (`read_collection_len`).

## Validity assessment

The outer route-count defect is a true positive. Both `pg_routes` and
`historical_pg_routes` reserved `count * size_of::<StorageNodePgRoute>()`
before proving that the bounded representation contained `count` route
records.

The report's statement that the inner acting-set allocation was fully bounded
was incomplete. The old `fields.len() != 5 + acting_len` check evaluated an
unchecked addition first. An `acting_len` near `usize::MAX` could panic in an
overflow-checked build before reaching the mismatch rejection. Collecting an
entire route line into `Vec<&str>` also introduced avoidable representation
amplification.

As with RESEXHAUST-001, the original attacker framing was too strong. The
runtime config is service-owned durable state beneath a mode-`0700` data
directory and is decoded at startup/bind. An actor able to replace it can
already deny service by deleting or corrupting state. The finding is retained
as low-severity fail-closed parser hardening.

## Resolution

Fixed in
[`89c2c388cee08a7857878253d3bcd2584439fbc8`](https://github.com/justincormack/argmin/commit/89c2c388cee08a7857878253d3bcd2584439fbc8)
(`Harden storage runtime config decoding`).

The loader now enforces one bounded, no-follow, nonblocking regular-file read
before decoding, and persistence enforces the same maximum encoded size. Both
route collections validate their count against the remaining bounded records
and use incremental allocation rather than trusting the declared count.

Each route is parsed directly from `split_whitespace` without collecting an
intermediate field vector. Acting-set nodes are appended only as actual fields
are parsed, extra fields are rejected immediately, and the final parsed count
is compared directly with `acting_len`; no `5 + acting_len` arithmetic remains.
The same commit also makes staging publication symlink-safe and rejects FIFOs
without blocking before regular-file validation.

The deterministic regressions cover oversized current and historical route
counts, `acting_len = usize::MAX`, oversized files, FIFO and non-regular input,
read and staging symlinks, stale regular staging recovery, and canonical
round trips. The fixing commit passed the complete workspace nextest suite
(7,628 tests) and Clippy across all targets and features with warnings denied.
