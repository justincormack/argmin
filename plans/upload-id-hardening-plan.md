# UploadId Hardening Plan

## Status

In progress.

This is the next identifier-boundary hardening target after the completed
bucket/object name rewrite.

## Trigger

`UploadId` still has the same broad "string wrapper" shape that bucket/object
names had before the recent hardening work:

1. [crates/storage/src/types.rs](/home/justin/src/github.com/justincormack/argmin/crates/storage/src/types.rs)
   defines it through the permissive `string_newtype!` macro.
2. `uploadId` values still enter through raw query parsing in
   [crates/server-http/src/http/request.rs](/home/justin/src/github.com/justincormack/argmin/crates/server-http/src/http/request.rs).
3. Multipart HTTP/coordinator/storage call paths still mostly pass `&str`
   upload identifiers.
4. Low-level multipart keying and hashing in
   [crates/server-core/src/pg.rs](/home/justin/src/github.com/justincormack/argmin/crates/server-core/src/pg.rs)
   still accept unchecked string upload IDs.

This is lower risk than bucket/object names because `UploadId` is an opaque
token rather than a namespace identifier, but it is still an externally
supplied identifier that crosses real routing, storage, and hashing
boundaries.

## Decision

Treat `UploadId` as a real validated domain type rather than an arbitrary
string wrapper.

The design target is:

1. parse `uploadId` once at the HTTP boundary
2. hold `UploadId` through coordinator/storage/hash paths
3. remove implicit string behavior (`From<&str>`, `From<String>`,
   `Deref<Target = str>`) from production use
4. keep actual string boundaries explicit through `.as_str()`

This should be a narrower follow-up than the bucket/object rewrite:

1. `UploadId` remains an opaque token
2. validation should be bounded and conservative rather than trying to model
   a rich documented AWS identifier grammar we do not actually have
3. `SessionId` can be cleaned up as a nearby internal follow-on, but it is not
   the primary external hardening target

One important compatibility constraint is different from bucket/object names:

1. AWS-visible failure behavior for present-but-arbitrary `uploadId` values is
   not yet pinned down by this plan
2. current multipart behavior largely treats a present `uploadId` as an opaque
   token and lets misses fall through to `NoSuchUpload`
3. this plan should not assume that "weird but present" upload IDs become
   parser-level `InvalidArgument` errors without explicit AWS verification

## Why this work is needed

The current shape keeps a footgun alive in the multipart path:

1. the type name suggests structure, but the type does not currently enforce
   any invariant
2. raw query-derived upload IDs flow through request/coordinator/storage
   boundaries as plain strings
3. low-level multipart keying still accepts raw string upload IDs, so explicit
   boundary enforcement is not visible in the type system
4. the codebase now has a clearer "validated identifier types with explicit
   string boundaries" direction for bucket/object names, and `UploadId` is the
   next obvious inconsistency

Even if AWS treats upload IDs as opaque, we should still avoid broad implicit
string behavior and unbounded unchecked propagation for externally supplied
multipart identifiers.

## Goals

1. Make `UploadId` an explicit bounded domain type.
2. Parse `uploadId` once at the HTTP boundary and keep it typed through
   multipart request handling.
3. Remove implicit string conversions from `UploadId`.
4. Move multipart storage/hash helpers to typed `UploadId` entry points.
5. Add regression coverage for malformed or oversized `uploadId` values.

## Non-goals

1. Reworking the multipart feature set itself.
2. Introducing a speculative AWS-specific upload-ID grammar beyond what is
   needed for bounded safe handling.
3. Folding `SessionId` hardening into the same patch set unless it materially
   simplifies the `UploadId` rewrite.

## Proposed validation model

`UploadId` should remain opaque but no longer arbitrary.

The target internal contract should be based on observed AWS-issued upload IDs:

1. Argmin-generated `UploadId` values should use the same general size range
   and character family as AWS-issued upload IDs.
2. `UploadId` should still become a bounded typed token internally, with an
   explicit invariant chosen to match that AWS-shaped generated form.
3. The implementation should avoid inventing a stricter external grammar than
   AWS actually exposes.

The important part is that upload IDs become an auditable bounded type
internally rather than a free-form string wrapper.

## Failure model and compatibility rule

This plan needs to keep two questions separate:

1. what invariant the server stores and uses internally for `UploadId`
2. what wire-visible behavior Argmin preserves for client-supplied present
   `uploadId` tokens

The intended rule is:

1. generated/stored `UploadId` values should satisfy an explicit bounded type
   invariant aligned with observed AWS-issued upload IDs
2. production code should use typed `UploadId` internally instead of raw
   strings
3. but HTTP behavior for present multipart query tokens must preserve current
   AWS-visible behavior unless AWS verification explicitly justifies a parser
   rejection

AWS verification since writing the initial draft clarified the split:

1. AWS-issued multipart upload IDs observed in practice are `128` characters
   long and use the character family `[A-Za-z0-9._]`
2. `upload-id-marker` is validated by AWS as a real pagination cursor:
   - a real issued marker is accepted
   - truncating, appending, or mutating a real marker returns
     `400 InvalidArgument` with `Invalid uploadId marker`
3. main multipart operation `uploadId` parameters still appear to be closer to
   opaque lookup tokens, where Argmin should preserve current `NoSuchUpload`
   miss behavior unless separate AWS verification shows otherwise

So the final design does need a split:

1. generated/stored IDs follow the AWS-observed `128`-character `[A-Za-z0-9._]`
   shape
2. `upload-id-marker` should be validated at the HTTP boundary and rejected as
   `InvalidArgument` when malformed
3. main multipart operation `uploadId` values can remain raw at the outer HTTP
   boundary for now if that is required to preserve `NoSuchUpload` semantics,
   but the hardened conversion point must still be explicit and typed
4. "missing uploadId" and "present but unknown uploadId" must remain distinct

## Work Plan

## Phase 1: Make UploadId a strict type

1. Rework `UploadId` in
   [crates/storage/src/types.rs](/home/justin/src/github.com/justincormack/argmin/crates/storage/src/types.rs)
   to use fallible construction with a dedicated validation error.
2. Remove infallible `From<&str>` / `From<String>` construction for untrusted
   input.
3. Remove `Deref<Target = str>` from `UploadId`.
4. Decide and implement the persistence/load rule for `UploadId` `FromSql`
   values.
5. Make an explicit call on how internal `UploadId` invariants relate to
   current external wire behavior for present multipart query tokens, using
   the AWS-issued size range and character family as the target for generated
   IDs.

Current state:

1. Done: `UploadId` now uses fallible typed construction rather than the old
   permissive string wrapper.
2. Done: the internal invariant is now the AWS-observed generated shape:
   exactly `128` characters from `[A-Za-z0-9._]`.
3. Done: production code no longer gets infallible `From<&str>` / `From<String>`
   or `Deref<str>` behavior for `UploadId`.
4. Done: `FromSql` now revalidates stored values through the typed constructor.
5. Done: Argmin-generated multipart upload IDs now use the same observed AWS
   size and character family instead of the earlier `32`-char lowercase hex
   shape.
6. Done: the plan’s compatibility rule is now explicit:
   - malformed `upload-id-marker` is an AWS-verified `InvalidArgument`
     rejection
   - main multipart operation `uploadId` behavior is still preserved as a
     separate seam until independently verified

Phase 1 is complete.

Exit criteria:

1. `UploadId` cannot be built casually from arbitrary strings.
2. Stored multipart rows have one explicit audited load rule.
3. The plan records whether present arbitrary client tokens are still allowed
   to fall through to `NoSuchUpload` on the wire.

## Phase 2: Type the HTTP/coordinator multipart boundary

1. Change shared HTTP multipart query parsing helpers so the hardened boundary
   is explicit for:
   - `uploadId`
   - `upload-id-marker`
2. If AWS-visible behavior requires it, allow the low-level parser to preserve
   a raw present token long enough for a shared `server-http` helper to choose
   the correct wire-visible mapping before converting to typed `UploadId`.
3. Update buffered multipart request handling in
   [crates/server-http/src/http/mod.rs](/home/justin/src/github.com/justincormack/argmin/crates/server-http/src/http/mod.rs)
   to use typed upload IDs.
4. Update streaming multipart request state/bindings to carry `UploadId`
   instead of raw `String`.
5. Push typed `UploadId` through coordinator request wrappers and helper APIs,
   including `ListMultipartUploads` marker handling and response-side
   `next_upload_id_marker` shaping where those values are part of the external
   multipart boundary.

Current state:

1. Done: `ListMultipartUploads` validates `upload-id-marker` at the HTTP
   boundary, passes typed `UploadId` markers through coordinator/storage, and
   returns AWS-matching `InvalidArgument` for malformed markers.
2. Done: the main multipart operation `uploadId` boundary is now split cleanly
   at the HTTP layer:
   - missing `uploadId` is still `InvalidRequest`
   - malformed-present `uploadId` is resolved in `server-http` before
     coordinator dispatch
   - only typed `UploadId` values now cross into coordinator request wrappers
3. Done: streaming multipart request state and bindings now carry typed
   `UploadId` end to end rather than raw `String`.
4. Done: the AWS-visible invalid-present multipart `uploadId` behavior is now
   explicitly pinned for `AbortMultipartUpload`, `UploadPart`,
   `CompleteMultipartUpload`, and `ListParts`:
   - overlong present `uploadId` returns `404 NoSuchUpload`
   - the response uses the fixed AWS message
   - the submitted token is echoed in a separate `<UploadId>` element
   - for the tested cases, `NoSuchUpload` takes precedence over `AccessDenied`
5. Remaining: Phase 3 still needs to finish the storage/hash boundary audit so
   multipart low-level helpers stop taking raw upload-id strings where they
   model the external identifier.

Phase 2 is complete.

Exit criteria:

1. request paths do not carry raw multipart upload identifier tokens into
   coordinator logic without an explicit hardened boundary
2. multipart request state uses `UploadId` on both buffered and streaming
   paths
3. `upload-id-marker` / `next_upload_id_marker` are part of the same typed
   boundary rather than a half-typed list path

## Phase 3: Type the storage and hash boundary

1. Convert multipart storage trait methods from `&str upload_id` to
   `&UploadId` where they represent the external multipart identifier.
2. Update low-level multipart key/hash helpers in
   [crates/server-core/src/pg.rs](/home/justin/src/github.com/justincormack/argmin/crates/server-core/src/pg.rs)
   to take `&UploadId`.
3. Audit any remaining raw-string upload-id surfaces in production code and
   either type them or make the raw boundary explicit and justified.

Exit criteria:

1. unchecked raw upload IDs no longer reach multipart storage/hash helpers on
   the production path
2. the main multipart routing/storage boundary is typed end to end

## Phase 4: Coverage and closeout

1. Add unit tests for `UploadId` construction and persistence loading.
2. Add request-parser regressions for missing multipart query parameters and
   for any malformed/oversized values that are intentionally rejected by the
   final AWS-verified wire contract.
3. Add targeted multipart request tests for both buffered and streaming paths,
   plus `ListMultipartUploads` marker coverage (`upload-id-marker` and
   `next_upload_id_marker`).
4. Verify the observed AWS-issued size range and character family for upload
   IDs, and separately verify AWS-visible behavior for present-but-arbitrary
   `uploadId` and `upload-id-marker` values before introducing parser-level
   invalid-ID failures.
5. Extend parser fuzz coverage if needed so `uploadId` parsing and multipart
   query boundaries are exercised under the new typed model.
6. Decide whether to fold `SessionId` cleanup into this closeout or leave it as
   a smaller internal follow-up.

Exit criteria:

1. the final AWS-visible multipart identifier failure model is explicit and
   covered by tests
2. the new `UploadId` boundary is exercised at type, parser, multipart
   request, and list-marker levels

## Review Checklist

Each patch in this plan should be reviewed against:

1. Does any externally supplied `uploadId` still cross a boundary as `String`
   or `&str`?
2. Is the internal validation contract explicit and bounded?
3. Are HTTP error mappings preserved for missing vs present-but-invalid vs
   unknown multipart query parameters, with AWS verification where behavior is
   not already known?
4. Do multipart storage/hash helpers now take `&UploadId` rather than raw
   strings?
5. Has implicit string behavior actually been removed, rather than just adding
   another validator call upstream?

## Success Criteria

This work is done when all of the following are true:

1. `UploadId` means "already validated and bounded" in production code.
2. Raw multipart identifier tokens exist only at explicit HTTP/query mapping
   boundaries.
3. Multipart coordinator/storage/hash paths no longer rely on unchecked string
   upload IDs.
4. `upload-id-marker` / `next_upload_id_marker` follow the same explicit
   boundary model as the main `uploadId` path.
5. The wire-visible behavior for present multipart identifier tokens is
   explicit and AWS-verified where needed.
6. `SessionId` is either cleaned up at the same boundary or explicitly left as
   a separate internal-only follow-up with documented reasoning.
