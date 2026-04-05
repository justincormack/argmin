# Requester Pays Note

This is a short future-work note, not an implementation plan.

## Current position

Argmin does not implement requester-pays behavior now.

That is acceptable for the current roadmap, but it is a real AWS compatibility
gap rather than a purely optional client feature.

## Why keep this note

Recent AWS CLI review was a useful reminder that requester-pays still appears
in current client surfaces, even if the CLI integration test only lightly
checks it.

The more important point is that requester-pays has real wire semantics:

- bucket-level requester-pays configuration
- `x-amz-request-payer: requester` on requests that require it
- `x-amz-request-charged: requester` on responses where AWS returns it
- different behavior for bucket owner versus non-owner callers

So this should be treated as a compatibility feature, not just a CLI flag.

## Testing note

If we implement this later, the important tests must be cross-account.

A single-account happy-path test is not enough. The useful conformance cases
need at least:

- bucket owner account
- alternate account with permission to access the bucket
- the same request with and without `x-amz-request-payer: requester`

That is the only way to verify the actual AWS behavior instead of only checking
that clients can send the header.

## If revisited later

Start with the narrowest AWS-visible surface:

- bucket requester-pays configuration API
- read/write operations that require the requester-pays header for a
  non-owner account
- response `x-amz-request-charged` behavior

Only after that should we worry about broader client ergonomics or higher-level
CLI coverage.
