<!-- Copyright The Argmin Authors. -->
<!-- SPDX-License-Identifier: CC-BY-4.0 -->

# Security Testing

## Scope

This guide defines the local security-focused test entry points for the repo.
It complements rather than replaces:

- `./scripts/coverage` for local integration coverage movement
- AWS-backed compatibility suites and golden shape assertions in `s3-tests`
  and `sts-tests`
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
- `lifecycle`: Object Lock, retention, legal-hold, and lifecycle coverage
- `transport`: HTTP-only and secure-transport-sensitive behavior
- `redaction`: secret redaction and error sanitization coverage
- `integrity`: storage and checksum integrity coverage
- `all`: the full local deterministic security suite

Useful commands:

```bash
./scripts/security-tests fast
./scripts/security-tests authn authz
./scripts/security-tests parser
./scripts/security-tests stateful
./scripts/security-tests lifecycle
./scripts/security-tests transport
./scripts/security-tests redaction
./scripts/security-tests integrity
./scripts/security-tests all
```

## Surface Matrix

| Surface | Local deterministic coverage | Oracle / robustness coverage | Findings / plans | Current gap |
| --- | --- | --- | --- | --- |
| Request parsing and protocol boundary hardening | `./scripts/security-tests parser` | `./scripts/fuzz auth_dates auth_post auth_request server_http_parsers server_http_post_multipart server_http_chunked_decoder server_http_streaming_frontend` | `plans/completed/parser-hardening-plan.md`, `security/codex-058e075`, `security/codex-14d95e1`, `security/codex-33d8271`, `security/codex-f6004f1` | Per-finding verification is linked from [the inventory](security-findings-inventory.md). |
| SigV4 header auth, presigned auth, POST auth, and streaming signatures | `./scripts/security-tests authn` | `./scripts/aws-tests --test headers`, `./scripts/aws-tests --test presigned`, `./scripts/aws-tests --test post_object`, `./scripts/aws-tests --test chunked`, `./scripts/fuzz auth_post auth_request` | `plans/completed/aws-auth-oracle-coverage-plan.md`, `security/codex-172cf0a`, `security/codex-2551b28` | No separate CI job yet; AWS-backed oracle coverage remains outside the local suite. |
| Authorization and discovery behavior | `./scripts/security-tests authz` | `./scripts/aws-tests --test object_write_constrained`, `./scripts/aws-tests --test boe_constrained`, `./scripts/aws-tests --test ownership` | `plans/completed/authz-model-testing-plan.md`, `security/codex-23ffb1b`, `security/codex-bb033e7` | Discovery coverage is still grouped under `authz` rather than its own dedicated runner group. |
| Bucket-policy parsing, evaluation, and enforcement | `./scripts/security-tests authz` | `./scripts/aws-tests --test bucket_policy`, `./scripts/aws-tests --test bucket_policy_conditions`, `./scripts/aws-tests --test bucket_policy_root` | `plans/completed/bucket-policy-differential-testing-plan.md`, `security/codex-7acfc72`, `security/codex-cece86a` | The bucket-policy condition matrices now also run locally in the default suite; AWS runs validate the same golden outcomes. |
| Multipart upload and streamed write lifetime behavior | `./scripts/security-tests stateful` | `cargo test -p s3-tests --test multipart`, `cargo test -p s3-tests --test chunked` | `plans/completed/multipart-reclaim-stream-session-testing-plan.md`, `security/codex-470aa8a`, `security/codex-195d31f` | Good local stateful coverage exists, but the matrix is still grouped broadly rather than by individual failure mode. |
| Reclaim queue and payload lease behavior | `./scripts/security-tests stateful` | Covered through multipart/reclaim stateful/property coverage; historical design record in `plans/completed/reclaim-queue-hardening-plan.md` | `security/codex-ecef3a4`, `plans/completed/reclaim-queue-hardening-plan.md` | Still grouped under broader stateful coverage rather than a dedicated reclaim-only runner group. |
| Object Lock, retention, legal hold, and lifecycle interactions | `./scripts/security-tests lifecycle` | `cargo test -p s3-tests --test object_lock`, `cargo test -p s3-tests --test lifecycle` | `plans/completed/object-lock-compat-plan.md`, `security/codex-1f0908f`, `security/codex-cd48ea6` | AWS oracle coverage remains separate from the local suite. |
| Transport constraints such as HTTP vs secure transport for `SSE-C` | `./scripts/security-tests transport` | `cargo test -p s3-tests --test sse_c` | `plans/completed/sse-c-direct-tls-enforcement.md`, `security/codex-32d164e` | Current local transport group is still narrow and focused on HTTP-only paths. |
| Observability and secret redaction | `./scripts/security-tests redaction` | Advisory code review via `./scripts/check-log-hotspots` | `plans/completed/observability-formatting-hardening-plan.md`, `plans/production-observability-migration-plan.md`, `security/codex-a413960`, `security/codex-90f9972` | Still a local-only signal; there is no separate CI job yet. |
| Storage integrity and corruption handling | `./scripts/security-tests integrity` | `cargo test -p s3-tests --test checksums`, `cargo test -p s3-tests --test request_checksums` | `plans/completed/segment-integrity-checks-for-bounded-reads.md`, `plans/completed/request-checksum-compat-plan.md`, `security/codex-8f1ed6f`, `security/codex-ae442f5` | Some bounded listing regressions still live as explicit `cargo test` commands in the finding inventory instead of dedicated runner groups. |

## Finding Inventory

Per-finding coverage now lives in
[`guides/security-findings-inventory.md`](security-findings-inventory.md).
That file is the one-row-per-finding map for every file under `security/`,
including invalid and stale findings.

## Notes

- `./scripts/security-tests` is intentionally local and deterministic. It does
  not include AWS-backed suites or long-running fuzz sessions by default.
- `./scripts/fuzz` remains the front door for parser robustness work.
- `./scripts/coverage` remains the front door for local integration coverage
  movement; it is not the security-suite runner.

## Current Residual Gaps

- CI wiring is intentionally deferred; this remains a local-only runner and
  guide.
- Per-surface reporting is still just the sectioned output from
  `./scripts/security-tests`; there is not yet a dedicated report wrapper.
