# Observability Formatting

This guide defines how tracing and future logging must handle attacker-controlled
text and security-sensitive values.

The main rule is simple:

- `Display` remains protocol-facing and may stay raw when the S3 surface needs it
- `Debug` is the observability-safe representation

Use the observability-safe form by default in traces and diagnostics.

## Threat Model

There are two separate risks:

1. attacker-controlled text can inject structure into logs if emitted raw
2. secrets or bearer material can be disclosed even if escaped correctly

These require different treatment.

## Formatting Rules

### Use escaping for non-secret attacker-controlled text

For values such as:

- bucket names
- object keys
- upload IDs
- session IDs
- request paths
- header names

preserve the value, but render it with quoted escaped formatting so control
characters cannot forge log structure.

Preferred approaches:

- implement a custom `Debug` that uses `observability::escaped(...)`
- log with `{:?}` rather than `{}`

### Use redaction for secrets and bearer material

Never log raw values for:

- query strings carrying presigned SigV4 material
- authorization-like fields
- session tokens
- signatures and signing keys
- SSE-C keys and derived secret material
- wrapped keys, encrypted checksums, and similar persisted encryption blobs

Preferred approaches:

- `observability::redacted("label")`
- presence bits
- counts or lengths
- carefully chosen non-sensitive summaries

If a value would be dangerous when copied from logs, it should be redacted, not
escaped.

## When Adding New Types

When adding a new type, decide whether it can appear in traces, errors, panic
output, or diagnostics.

If yes:

1. decide whether the fields are attacker-controlled, secret, both, or neither
2. do not rely on derived `Debug` for attacker-controlled or secret-bearing
   fields
3. add a custom `Debug` impl that:
   - escapes attacker-controlled text
   - redacts secrets
   - summarizes large raw blobs instead of dumping contents
4. keep `Display` unchanged unless the protocol surface itself is wrong

Good patterns:

- textual identifiers: quoted escaped `Debug`
- secret-bearing structs: explicit field allowlist in manual `Debug`
- raw blobs/config documents: length or presence summaries

## When Adding Trace or Log Sites

Before adding a field to a trace line:

1. prefer structured non-sensitive summaries over raw protocol strings
2. use hardened types with `{:?}` where possible
3. do not format request queries, credentials, or encryption material directly
4. for errors, prefer stable error codes over full error text when the error may
   contain request-derived values

Good examples:

- `has_query=true`
- `query_params=4`
- `sigv4_query=true`
- `error_code=InvalidRequest`

Bad examples:

- raw `query=...`
- raw `credential=...`
- raw `key={}`
- full error formatting when the error text embeds request material

## Review Checklist

Use this during review for new observability code:

1. Does any `trace!`-style formatting use `{}` with attacker-controlled text?
2. Does any derived `Debug` include attacker-controlled `String`, `Vec<u8>`, or
   secret-bearing fields?
3. Are secrets redacted rather than merely escaped?
4. Are large raw blobs summarized instead of printed?
5. Do tests cover escaping and redaction for the new type or trace helper?

## Scope of Existing Hardening

The initial hardening work was delivered in:

- `7085c81` `Harden trace formatting for request metadata`
- `8d37645` `Harden observability debug formatting`
- `aeb1180` `Complete observability formatting hardening`

Those changes established the current baseline. New code should follow this
guide instead of reintroducing raw formatting and then fixing it later.
