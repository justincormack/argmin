# SSE-C Direct TLS Enforcement Plan

## Goal

Add direct TLS support to `argmin-s3` and enforce AWS's requirement that any
request using `SSE-C` must arrive over HTTPS.

Out of scope for this plan:

- proxy / load-balancer forwarded scheme handling
- trusting `X-Forwarded-Proto` or similar headers
- broader TLS deployment policy beyond direct server TLS

## Why This Is Needed

AWS requires `SSE-C` requests to use HTTPS because the customer key is sent in
request headers or form fields. Our current server only speaks plain HTTP, so
we cannot enforce this compatibility requirement yet.

## Current Status

This work is now implemented:

1. `argmin-s3` supports direct TLS with `ARGMIN_TLS_CERT_PATH` and
   `ARGMIN_TLS_KEY_PATH`.
2. `S3Request` carries typed transport security state.
3. SSE-C request parsing rejects insecure HTTP for header, copy-source, and
   POST-form SSE-C entry points.
4. The local `s3-tests` harness runs over HTTPS by default with a dedicated
   test CA and an explicit HTTP mode for negative tests.
5. Integration coverage exists for HTTPS success and HTTP rejection on SSE-C
   `PUT`, `GET`, `HEAD`, `POST Object`, and presigned requests.
6. [`guides/threat_model.md`](/home/justin/src/github.com/justincormack/argmin/guides/threat_model.md)
   has been updated to reflect direct TLS support and the current proxy
   out-of-scope boundary.

This plan is complete for its intended scope. Remaining future work, if needed,
would be separate proxy-termination support with an explicit trust model rather
than more direct-TLS enforcement work.

## Implementation Plan

### 1. Add direct TLS server config

Add explicit TLS config to `argmin-s3`, for example:

- `ARGMIN_TLS_CERT_PATH`
- `ARGMIN_TLS_KEY_PATH`

Behavior:

- if neither is set, server remains HTTP-only
- if both are set, server serves HTTPS directly
- partial config is an error

No proxy mode or forwarded-scheme config should be added in this phase.

### 2. Add typed transport security to requests

Extend `S3Request` with a small typed flag such as:

- `TransportSecurity::InsecureHttp`
- `TransportSecurity::Tls`

Set it once in the connection-handling path. The rest of the HTTP stack should
read this typed state rather than guessing from headers.

### 3. Enforce HTTPS for all SSE-C entry points

At the HTTP edge, reject insecure requests that use:

- `x-amz-server-side-encryption-customer-*`
- `x-amz-copy-source-server-side-encryption-customer-*`
- SSE-C POST form fields

This should happen in the existing SSE-C parsing helpers so all affected APIs
inherit the same enforcement:

- `PutObject`
- `GetObject`
- `HeadObject`
- multipart initiate/upload/complete paths that use SSE-C headers
- `CopyObject`
- `UploadPartCopy`
- `POST Object`
- presigned SSE-C requests

`server-core` should not depend on transport state.

### 4. Add direct HTTPS integration coverage

Extend the local `s3-tests` server harness to start with a test certificate.
Add end-to-end tests for:

1. SSE-C `PUT` over HTTP fails
2. SSE-C `GET`/`HEAD` over HTTP fail
3. SSE-C `POST Object` over HTTP fails
4. presigned SSE-C over HTTP fails
5. the same operations succeed over HTTPS
6. non-SSE-C requests continue to work over HTTP

### 5. Verify AWS-compatible error behavior

Once direct TLS exists, confirm the exact AWS response shape for insecure
`SSE-C` requests as closely as practical and align our status/code/body.

### 6. Update the threat model

[`guides/threat_model.md`](/home/justin/src/github.com/justincormack/argmin/guides/threat_model.md)
currently reflects the older assumption that TLS is external to the server and
that direct TLS support is out of scope. As part of this work, update it to:

1. describe direct TLS as a supported deployment mode
2. remove the stale assumption that external TLS is the only intended approach
3. document that proxy TLS termination remains out of scope for now
4. call out that `SSE-C` over plain HTTP is rejected

## Notes

This plan intentionally avoids proxy support for now. If proxy TLS termination
is needed later, it should be a separate plan with an explicit trust model,
not an extension of this direct-TLS enforcement work.
