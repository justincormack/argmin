# Request Parser Hardening Plan

## Status

Completed.

Implemented in:

- `05c8412` `auth: start request parser hardening`
- `c937a53` `auth: harden shared sigv4 parser helpers`
- `d6c9fe2` `server-http: harden upload part query parsing`
- `71c3a59` `server-http: finish parser hardening sweep`
- `e3b43ee` `fuzz: add parser hardening targets`

This plan's original scope is complete. Any further parser work should be
tracked as new maintenance or feature-specific plans rather than extending this
one.

## Scope

This plan tracks a small, generic hardening pass over request parsing at the
HTTP/auth boundary.

The immediate trigger is a class of security bugs caused by treating
attacker-controlled protocol fields as ad hoc `&str` values and then slicing,
indexing, or numerically iterating on them without first converting them into
validated domain types.

This plan is intentionally narrow:
- request/auth parser safety
- bounded parsing of protocol-shaped strings
- reusable typed helpers for wire formats
- regression coverage for malformed and adversarial inputs

This plan does not cover:
- XML compatibility work in general
- bucket policy language changes
- authz semantics changes
- a broad rewrite of `server-http`

## Motivation

Recent security findings have the same shape:

1. a wire-format field is accepted as a plain `&str`
2. downstream code assumes structure that was never validated
3. the code slices by byte offset, indexes by parsed numeric values, or loops
   according to attacker-controlled numbers
4. malformed input causes panic, overflow, or unbounded work

Examples already seen:
- POST policy expiration parsing in `crates/auth/src/post.rs`
- POST SigV4 `x-amz-date` handling in `crates/auth/src/post.rs`

The fix should be systematic rather than one bug at a time.

## Goals

1. parse protocol-shaped strings exactly once at the boundary
2. convert parsed values into small validated types before business logic uses
   them
3. reject non-ASCII or structurally invalid inputs before any byte slicing
4. ensure parsed numeric components are range-checked before indexing or
   arithmetic
5. ensure parsing work is bounded independently of attacker-controlled field
   values
6. add regression tests for malformed UTF-8-adjacent and oversized inputs

## Non-goals

1. do not introduce a generic parser framework dependency
2. do not redesign all request parsing in one change
3. do not move HTTP parsing out of `server-http`
4. do not widen accepted wire formats beyond AWS behavior

## Design Rules

### 1. Parse once into validated types

Protocol fields should not travel through core/auth logic as raw `&str` if the
logic depends on structure.

Examples:
- `x-amz-date` -> `AmzDate`
- POST policy expiration -> `Iso8601Expiry`
- credential scope -> `CredentialScope`

The parser owns:
- structural validation
- ASCII checks where the protocol is ASCII-only
- numeric range checks
- bounded conversion to internal representation

Callers should only see validated values.

### 2. ASCII protocols should be parsed as ASCII

If AWS defines a field in an ASCII wire format, reject non-ASCII input before
doing any positional checks. This avoids `&str[..n]` and non-character-boundary
bugs entirely.

Preferred pattern:
- `let bytes = s.as_bytes();`
- length check
- fixed separator check
- digit check
- numeric parse from validated digit slices

Do not:
- slice untrusted `str` by byte index
- assume byte offsets are char boundaries

### 3. No loops or allocations proportional to parsed numeric fields

User-supplied years, counts, lengths, or indexes must not drive unbounded
loops or unchecked indexing.

Preferred pattern:
- closed-form checked conversion helpers
- explicit maximum width for numeric fields
- `checked_*` arithmetic for derived values

Do not:
- iterate `for _ in 0..user_value`
- index arrays with parsed values before validation

### 4. Reuse existing typed helpers before adding new ad hoc parsers

There is already useful parsing logic in the tree, for example
`auth::canonical::parse_amz_date`. Harden and reuse these helpers instead of
growing multiple incompatible parsers for the same wire format.

## Initial Target Surfaces

### Phase 1: Auth date parsing

Target:
- `crates/auth/src/post.rs`

Deliver:
- stop handling POST SigV4 `x-amz-date` as an ad hoc string prefix
- reuse or extract a shared validated `AmzDate` parser
- add malformed UTF-8-boundary regression coverage

Success criteria:
- no raw `[..8]` slicing on untrusted POST date input
- malformed multibyte `x-amz-date` returns an auth error, not panic
- unit tests cover short, malformed, non-ASCII, and bad-value cases

### Phase 2: Shared wire-format helpers

Targets:
- `crates/auth/src/canonical.rs`
- `crates/auth/src/post.rs`
- selected `server-http` helpers

Deliver:
- a small shared set of parser helpers for:
  - AWS basic timestamp/date forms
  - SigV4 credential scope strings
  - bounded ASCII token extraction where needed
- eliminate duplicate date/timestamp parsing logic where formats are the same

Success criteria:
- one parser per wire format
- no duplicated ad hoc ASCII/date parsers for the same field family

### Phase 3: Hotspot sweep in auth and server-http

Targets:
- `crates/auth`
- `crates/server-http/src/http/mod.rs`
- `crates/server-http/src/http/request.rs`
- `crates/server-http/src/http/multipart.rs`

Sweep for:
- byte slicing on untrusted strings
- `split`/`split_once` followed by unchecked positional assumptions
- indexing with parsed user values
- loops bounded by attacker-controlled numeric fields

Deliver:
- replace the high-risk cases with validated helpers
- add regression tests for each bug class found

Current progress:
- shared `UploadPart` query parsing now validates `uploadId`/`partNumber` for
  both streaming and buffered paths in `server-http`, removing a fallback path
  where malformed streaming `UploadPart` queries could bypass early validation
- repeated request-derived integer parsing in `server-http/http/mod.rs` now
  goes through shared helpers instead of local ad hoc parsing at each call site
- raw query walking for routing and copy-source `versionId` extraction now goes
  through shared `server-http/http/request.rs` helpers, removing the remaining
  duplicated `split('&')` / `splitn('=')` parsers on live router paths

Success criteria:
- remaining raw string manipulations are either:
  - obviously safe after prior validation, or
  - localized inside parser helpers with tests

Phase 3 status:
- completed for the original hotspot sweep scope in `auth` and `server-http`
- no remaining high-risk request parser sites were found in the final router /
  query pass; remaining parser work, if any, is maintenance-oriented rather
  than tied to a known bug class from this plan

### Phase 4: Parser fuzzing

Targets:
- `crates/auth`
- `crates/server-http`

Focus areas:
- SigV4 date and timestamp parsing
- SigV4 credential scope parsing
- POST policy expiration parsing
- presigned request query parsing
- shared query parameter parsing
- copy-source parsing
- multipart form field parsing
- XML timestamp and numeric field parsing used by Object Lock and related APIs

Deliver:
- add fuzz targets for parser helpers and narrow parser entry points rather
  than trying to fuzz the whole HTTP server end-to-end
- seed fuzz corpora with malformed UTF-8-adjacent ASCII, oversized numeric
  fields, invalid ranges, duplicate query parameters, and delimiter edge cases
- assert parser-level invariants such as:
  - no panics
  - no unbounded or pathological work from oversized numeric inputs
  - malformed inputs map to bounded parser/auth/request errors rather than
    internal errors

Success criteria:
- the parser surfaces involved in the earlier security findings all have
  dedicated fuzz targets
- fuzzing exercises malformed structural input beyond the current regression
  tests
- any newly discovered parser panic or pathological-work case feeds back into
  the shared helper set and regression suite

Phase 4 status:
- completed for the original parser-hardening scope

Current progress:
- added a `cargo-fuzz` harness under `fuzz/`
- initial fuzz targets cover:
  - auth date/timestamp parsing
  - auth POST SigV4 and POST policy parsing
  - auth request/presigned request parsing entry points
  - server-http routing and URL-encoded tag parsing

## Implementation Notes

Recommended order:

1. fix the POST SigV4 `x-amz-date` path using a validated helper
2. move the helper to the narrowest shared location that avoids duplication
3. audit adjacent auth parsers for the same bug shape
4. only then widen the sweep into `server-http`

Recommended code review rule:
- any new parser code operating on request-derived `&str` must justify:
  - why raw strings are kept instead of a validated type
  - why any positional slicing/indexing is safe

## Test Plan

Unit tests:
- malformed UTF-8-adjacent ASCII cases
- wrong length
- wrong separators
- non-digit characters in fixed-width numeric fields
- invalid month/day/hour/minute/second ranges
- oversized numeric fields intended to provoke large work

Targeted tests:
- `cargo test -p auth`
- targeted `server-http` tests for request parsing helpers touched by the sweep

Nice follow-up:
- add fuzz/property coverage for parser helpers that consume attacker-shaped
  protocol strings

## First Work Item

Implement Phase 1 first:
- replace POST SigV4 `x-amz-date` prefix slicing with a validated parser helper
- add a regression test for a multibyte input where byte index 8 falls inside a
  codepoint

That is the smallest change that advances the generic plan rather than just
patching one panic site in isolation.
