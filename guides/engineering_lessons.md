# Engineering Lessons

*Lessons learned building the EC engine. Generalise before adding new entries.*

---

## API Design

### Callers own all buffers; the library owns nothing

Every method that produces output takes caller-provided `&mut [u8]` slices.
No method allocates on the caller's behalf, even for "convenience" or
"background" operations.

Rationale: a caller doing a scrub pass over millions of objects should
allocate a scratch buffer once and reuse it across the whole scan. An API
that allocates internally forces per-call heap churn and prevents reuse.
The caller always knows the access pattern; the library never does.

Corollary: if a method needs temporary working space (e.g. `verify` needs
space to re-encode parity before comparing), expose a `foo_scratch_size()`
helper so callers know exactly how much to allocate. Return
`Err(ScratchTooSmall)` if the provided buffer is too small.

### Push size information to the caller

The library should not compute "how big a buffer do I need?" internally and
then allocate. It should expose that calculation as a public pure function
(`verify_scratch_size`, `encode_output_size`, etc.) so the caller can
allocate once at the right size. This keeps the library zero-allocation and
gives callers predictable memory usage.

### Error types must not allocate

All `Error` enum variants must carry only fixed-size data (`usize`, `&'static str`,
primitive integers). No `String`, no `Vec`, no `Box<dyn Error>` in any
variant. Returning an error must be as cheap as returning `Ok`.

### Internal APIs can be stricter than external ones

This is an internal API (not public-facing S3). Require sorted indices rather
than sorting internally, require exact buffer sizes, require callers to
compute scratch sizes. The cost of a wrong call is an immediate `Err`, not
silent misbehaviour. Save the ergonomics budget for the user-facing layer.

---

## C Library Integration (ISA-L, and by extension any C library)

### Read the source; distrust summaries

AI-generated summaries of C library behaviour can be wrong in subtle ways.
For ISA-L, the summary described the SIMD loop exit condition incorrectly.
The only reliable source is the header and the assembly. When a C function's
behaviour is load-bearing (correctness, safety, buffer bounds), read the
actual source.

### Verify parameter conventions with a trivial test before building on them

`gf_gen_cauchy1_matrix(a, m, k)` generates the *full* `(m+k) × k` systematic
encoding matrix — the first `k` rows are the identity, the last `m` rows are
the Cauchy coefficients. We initially called it as `(a, m, k)` expecting
only the `m` parity rows, and got identity rows back. The encode step
appeared to work (parity happened to be a copy of the first `m` data shards)
but reconstruction was provably singular.

Before building any non-trivial logic on top of a C function, write a tiny
test that prints or asserts the raw output for a known input. One-line
mistakes in parameter interpretation can corrupt an entire subsystem silently.

### Unsafe boundary: one crate, no leakage

All `unsafe` FFI lives in the `*-sys` crate. The public `ec` crate is safe
Rust. No `unsafe` block appears outside the FFI layer. This keeps the
audit surface small and the safe API surface checkable by the compiler.

### Cast `*const` to `*mut` explicitly, with a comment

ISA-L reads from "data" pointers but declares them `unsigned char **` (not
const-qualified). Cast `s.as_ptr() as *mut u8` at the call site and leave
a comment: `// ISA-L only reads from data pointers during encode`. This
makes it clear the cast is intentional and what contract we are relying on.

---

## Testing

### Write exhaustive combination tests first

For any system with MDS (maximum distance separable) properties, write a
test that enumerates every valid `C(k+m, k)` subset of shards and verifies
reconstruction. Run this before property tests. It is cheap (largest case
C(12,8) = 495 iterations), completely deterministic, and catches the most
fundamental correctness bugs immediately. We found the `gf_gen_cauchy1_matrix`
parameter bug this way within the first test run — property tests would have
taken much longer to pin down.

### Zero-length inputs are a first-class case, not an edge case

Test `shard_size = 0` explicitly and early. Empty objects exist in real
systems (zero-byte files, empty Docker layers). ISA-L treats length-0 as a
no-op. If a code path special-cases zero and returns an error, the caller
must branch; if it is a transparent no-op, the caller does not. Make the
no-op behaviour explicit in the test suite so it cannot regress.

### Allocation counting tests use thread-local storage, not global atomics

A counting allocator that uses a global `AtomicBool` flag will count
allocations from other threads running concurrently in the test harness,
producing false failures. Use `thread_local! { static COUNTING: Cell<bool> }`
so only the current test thread's allocations are counted. This lets
allocation tests run in the default parallel test harness without
serialisation.

### Proptest panics inside the test body produce confusing crash symptoms

If `.unwrap()` is called inside a `proptest!` body, a failure produces a
`SIGABRT` / heap corruption message rather than a clean proptest failure
message, because proptest's shrinking loop does not expect panics. Use
`prop_assert!` / `prop_assert_eq!` (which return `Err` and let proptest
shrink normally) instead of `assert!` / `.unwrap()` for assertions that
could legitimately fail on generated inputs.

---

## Storage System Specifics

### Stripe externally; the codec is stateless

The EC codec operates on fixed-size byte slices. For large objects (up to 5 GB),
the caller is responsible for streaming the object through the codec in
stripes (e.g. 4 MB per shard). The codec has no streaming interface and
requires no state between stripes. This is intentional: it keeps the codec
simple, testable in isolation, and lets the caller control memory usage
precisely. Document the stripe size recommendation in the layer that
interfaces with storage nodes, not in the codec.

### Reconstruction uses the full systematic encoding matrix directly

Store `gf_gen_cauchy1_matrix(a, k+m, k)` — the complete `(k+m) × k` matrix —
not just the parity sub-matrix. For reconstruction, select `k` rows by
present shard indices, invert, and multiply. There is no need to rebuild the
full matrix at decode time: the identity rows (for data shards) and Cauchy
rows (for parity shards) are all present in the stored matrix. Storing only
the parity rows and reconstructing the identity block dynamically is error-
prone and was the source of the initial matrix bug.

### Plans must survive contact with the actual library API

The design plan for the EC engine assumed `gf_gen_cauchy1_matrix` produced
only the Cauchy parity rows. The implementation revealed it produces the full
systematic matrix. Update plan documents when the library's actual API
differs from what was assumed. A plan that is known to be wrong is worse than
no plan.
