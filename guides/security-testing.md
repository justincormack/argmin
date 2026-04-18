# Security Testing

## Scope

This guide defines the local security-focused test entry points for the repo.
It complements rather than replaces:

- `./scripts/coverage` for local integration coverage movement
- AWS-backed compatibility and differential suites such as `s3-tests` and
  `s3-diff-tests`
- parser fuzzing under `fuzz/`

The default local entry point is:

```bash
./scripts/security-tests
```

This currently defaults to the `fast` group.

## Runner Groups

The local runner is intentionally simple and dependency-light. Current groups:

- `fast`: high-signal local checks for normal development
- `authn`: SigV4 header, presigned, and POST authentication coverage
- `authz`: authorization, bucket-policy, and anonymous/public-access coverage
- `parser`: request/parser/front-door hardening coverage
- `stateful`: multipart, reclaim, and stream-session lifetime coverage
- `transport`: HTTP-only and secure-transport-sensitive behavior
- `all`: the full local deterministic security suite

Useful commands:

```bash
./scripts/security-tests fast
./scripts/security-tests authn authz
./scripts/security-tests parser
./scripts/security-tests stateful
./scripts/security-tests transport
./scripts/security-tests all
```

## Surface Matrix

| Surface | Local deterministic coverage | Oracle / robustness coverage | Findings / plans | Current gap |
| --- | --- | --- | --- | --- |
| Request parsing and protocol boundary hardening | `./scripts/security-tests parser` | `./scripts/fuzz auth_dates auth_post auth_request server_http_parsers server_http_post_multipart server_http_chunked_decoder server_http_streaming_frontend` | `plans/completed/parser-hardening-plan.md`, `security/codex-058e075`, `security/codex-14d95e1`, `security/codex-33d8271`, `security/codex-f6004f1` | The local suite now has an entry point, but the security findings are not yet exhaustively backlinked one-by-one. |
| SigV4 header auth, presigned auth, and POST auth | `./scripts/security-tests authn` | `cargo test -p auth --test sigv4_aws_diff`, `cargo test -p s3-tests --test presigned`, `./scripts/fuzz auth_post auth_request` | `security/codex-172cf0a`, `security/codex-2551b28` | No separate CI job yet; AWS-backed oracle coverage remains outside the local suite. |
| Authorization and discovery behavior | `./scripts/security-tests authz` | `cargo test -p s3-tests --test access_matrix`, `cargo test -p s3-tests --test boe_constrained`, `cargo test -p s3-tests --test ownership` | `plans/completed/authz-model-testing-plan.md`, `security/codex-23ffb1b`, `security/codex-bb033e7` | The local runner does not yet split bucket-meta discovery checks into their own named group. |
| Bucket-policy parsing, evaluation, and enforcement | `./scripts/security-tests authz` | `cargo test -p s3-diff-tests --test bucket_policy`, `cargo test -p s3-tests --test bucket_policy`, `cargo test -p s3-tests --test bucket_policy_root` | `plans/completed/bucket-policy-differential-testing-plan.md`, `security/codex-7acfc72`, `security/codex-cece86a` | AWS differential runs remain separate from the default local suite. |
| Multipart upload and streamed write lifetime behavior | `./scripts/security-tests stateful` | `cargo test -p s3-tests --test multipart`, `cargo test -p s3-tests --test chunked` | `plans/completed/multipart-reclaim-stream-session-testing-plan.md`, `security/codex-470aa8a`, `security/codex-195d31f` | Good local stateful coverage exists, but the matrix is still grouped broadly rather than by individual failure mode. |
| Reclaim queue and payload lease behavior | `./scripts/security-tests stateful` | Targeted future follow-on from `plans/reclaim-queue-hardening-plan.md` | `security/codex-ecef3a4`, `plans/reclaim-queue-hardening-plan.md` | Still covered through the broader multipart/reclaim stateful suite rather than a dedicated reclaim-only group. |
| Object Lock, retention, legal hold, and lifecycle interactions | `cargo test -p s3-local-tests --test lifecycle`, selected `server-core` unit tests | `cargo test -p s3-tests --test object_lock`, `cargo test -p s3-tests --test lifecycle` | `plans/completed/object-lock-compat-plan.md`, `security/codex-1f0908f` | Not yet wired into a dedicated `scripts/security-tests` group. |
| Transport constraints such as HTTP vs secure transport for `SSE-C` | `./scripts/security-tests transport` | `cargo test -p s3-tests --test sse_c` | `plans/completed/sse-c-direct-tls-enforcement.md`, `security/codex-32d164e` | Current local transport group is still narrow and focused on HTTP-only paths. |
| Observability and secret redaction | `cargo test -p observability`, selected `auth`, `server-core`, and `server-http` unit tests with `debug_redacts`, `redacted`, or `sanitized` assertions | Advisory code review via `./scripts/check-log-hotspots` | `plans/completed/observability-formatting-hardening-plan.md`, `plans/production-observability-migration-plan.md`, `security/codex-a413960` | Not yet exposed as its own security-suite group. |
| Storage integrity and corruption handling | Selected `server-core`, `storage`, and `checksum` unit tests | `cargo test -p s3-tests --test checksums`, `cargo test -p s3-tests --test request_checksums` | `plans/completed/segment-integrity-checks-for-bounded-reads.md`, `plans/completed/request-checksum-compat-plan.md`, `security/codex-8f1ed6f` | Not yet wired into the local security runner. |

## Notes

- `./scripts/security-tests` is intentionally local and deterministic. It does
  not include AWS-backed suites or long-running fuzz sessions by default.
- `./scripts/fuzz` remains the front door for parser robustness work.
- `./scripts/coverage` remains the front door for local integration coverage
  movement; it is not the security-suite runner.

## Current Residual Gaps

- The matrix is now checked in, but it is still surface-oriented rather than a
  complete one-row-per-file inventory of every `security/` finding.
- Some important surfaces are only partially wired into the runner today:
  object lock and lifecycle, observability/redaction, and storage integrity.
- CI wiring is still pending; this is currently a local-only runner and guide.
