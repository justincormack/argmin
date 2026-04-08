# Observability Formatting Hardening Plan

## Context

Two security findings point at observability-formatting risks:

- `security/codex-28f87fb`: control characters in object keys can be emitted
  directly into trace output, enabling log injection or terminal control
  effects
- `security/codex-90f9972`: tracing can disclose security-sensitive request
  material such as presigned query parameters and credential-bearing auth
  fields

We do not have broad application logging yet, but we do have a tracing
framework that writes formatted fields verbatim to sinks. This is the right time
to establish formatting and redaction rules before more logging surfaces are
added.

## Problem

There are two distinct risks:

1. attacker-controlled text can be rendered unsafely
2. security-sensitive values can be rendered at all

These need different handling.

For attacker-controlled text:

- raw formatting into trace lines can preserve CR, LF, TAB, ESC, and other
  control characters
- derived `Debug` on string-bearing types prints their contents directly
- future logging code will repeat this mistake unless the safe default is in
  shared types and helpers

For security-sensitive values:

- some values should never appear in logs, even escaped
- examples include query strings containing presigned SigV4 material,
  authorization-like fields, SSE-C keys, wrapped-key material, and similar
  bearer or secret data

## Design Rules

### 1. `Display` stays protocol-facing, `Debug` becomes observability-safe

Do not repurpose `Display` for sanitization. Many types use `Display` for S3
protocol behavior, XML, headers, and errors where the raw value is part of the
actual API surface.

Instead:

- keep `Display` semantics unchanged unless a specific call site is wrong
- make `Debug` safe for developer-facing observability and diagnostics
- prefer `{:?}` or explicit observability wrappers for values that may be
  attacker-controlled

### 2. Unsafe text must be escaped, not dropped

For non-secret user-controlled text such as object keys, bucket names, upload
IDs, and session IDs:

- preserve the value semantically
- render with escaping for control characters and non-printing bytes
- make delimiters obvious so injected whitespace or newlines cannot forge log
  structure

The expected shape is quoted, escaped debug-style text.

### 3. Secrets and bearer material must be redacted, not escaped

For security-sensitive values:

- never emit the raw value to traces or logs
- prefer booleans, presence bits, counts, or fixed labels
- if identity is operationally useful, log a non-sensitive summary only when it
  is clearly justified

Examples:

- `query` -> `has_query=true` or a sanitized summary, not the raw query string
- SigV4 credential field -> presence or parsed non-sensitive metadata, not the
  raw credential string
- SSE-C key material -> always redacted

### 4. Centralize the policy in reusable helpers

Do not rely on every call site remembering how to sanitize each value.

We should add small shared helpers in `observability` or a nearby common module
for:

- escaped text rendering
- redacted values
- possibly summarized query/auth formatting where needed

Then types and trace sites should use those helpers consistently.

## Recommended Scope

Keep this work focused on the types and helpers most likely to become logging
surfaces soon.

## Status

### Completed

- Phase 1 is complete.
  - `observability` now provides shared escaped-text, redacted-value, and
    query-summary helpers.
  - `Redacted` now has observability-safe `Debug`, not just safe `Display`.
  - helper behavior is covered by focused unit tests.
- The first high-risk trace cleanup slice is complete.
  - request-start and auth traces no longer log raw query strings or SigV4
    credential material
  - request tracing now uses summarized query metadata instead of raw query
    text
- Phase 2 is complete for the core textual identifier newtypes most likely to
  appear in traces.
  - `BucketName`
  - `ObjectKey`
  - `UploadId`
  - `SessionId`
- Phase 3 has an initial hardening pass complete for obvious secret-bearing
  auth and SSE types, plus the remaining managed-encryption and request-error
  debug surfaces identified during review.
  - auth credential and SigV4 debug output now redacts secret key, session
    token, signing key, and signature material
  - SSE-C request, response-header, and write-context debug output now redacts
    customer-key-derived material
  - `AuthError` debug output now redacts unexpected security tokens and escapes
    attacker-controlled header names
  - managed encryption and persisted metadata/config debug output now uses
    summaries or redaction rather than printing raw wrapped keys, nonces,
    encrypted checksums, or raw config blobs
- Phase 4 has a broad high-risk trace formatting sweep complete across current
  bucket/key/upload/session trace sites.
  - these call sites now prefer `{:?}` on hardened types instead of raw `{}`
    formatting
  - this covers the main trace surfaces in `server-core`, `server-http`, and
    `storage`
  - remaining high-risk HTTP traces now avoid raw path and `Range` formatting,
    and response-body error traces log stable S3 error codes instead of
    formatting the full error text

### Remaining

- update
  `security/codex-28f87fb` and `security/codex-90f9972` with the shipped fix
  details and residual scope
- optionally do a later broad audit of less common diagnostic surfaces outside
  the current high-risk observability scope if logging expands materially

### Phase 1: Shared observability-safe formatting primitives

Add reusable wrappers or helper types for:

- escaped textual debug rendering
- redacted secret debug rendering
- optional query/auth summaries for request tracing

This phase should also document the intended usage pattern in code comments and
tests.

### Phase 2: Harden high-value string-bearing newtypes

Replace derived `Debug` on core textual identifier types with safe custom
`Debug`.

Initial target set:

- `BucketName`
- `ObjectKey`
- `UploadId`
- `SessionId`
- similar string newtypes that can be attacker-controlled and are likely to
  appear in traces

These should escape control characters and render with quotes.

### Phase 3: Harden security-sensitive types

Audit types that currently derive or implement `Debug` and may carry sensitive
material.

Likely targets include:

- auth request/credential parsing inputs
- SSE-C request/config/write-context types
- managed wrapping key config/provider types
- serialized metadata or encryption state blobs if they can contain sensitive
  material

Expected result:

- custom `Debug` that redacts secrets
- explicit allowlist of which fields may appear in debug output

### Phase 4: Trace call-site cleanup

After the shared primitives and type-level `Debug` are in place, clean up the
highest-risk trace sites so they stop formatting raw sensitive strings.

Initial examples:

- request-start traces
- auth traces
- streaming write event traces that include object keys or session labels

This phase should prefer structured summaries over raw copied protocol strings.

## Non-Goals

- building a full structured-logging subsystem now
- rewriting every existing error message immediately
- changing S3-visible protocol formatting
- introducing a large logging dependency

## Validation

Add focused tests for:

1. control characters in keys and IDs are escaped in `Debug`
2. secret-bearing types do not reveal sensitive bytes in `Debug`
3. observability helper wrappers produce one-line output with no raw CR/LF
   injection
4. high-risk trace call sites log redacted or summarized values instead of raw
   secrets

## Recommendation

This is worth doing before more tracing and logging is added.

The smallest useful first implementation is:

1. add shared escaped/redacted formatting helpers
2. switch the core string newtypes from derived `Debug` to safe custom `Debug`
3. clean up the currently known sensitive trace sites from
   `security/codex-28f87fb` and `security/codex-90f9972`

That gives us a real default safety improvement without trying to solve all
future observability design in one change.
