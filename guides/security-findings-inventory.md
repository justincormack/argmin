# Security Findings Inventory

This is the checked-in one-row-per-finding map for every file under
`security/`. The local path column is the durable local regression entry point
for the current local security suite.

## Request Parsing, Boundedness, and Transport

| Finding | Status | Local deterministic path | Notes |
| --- | --- | --- | --- |
| `security/codex-058e075` POST SigV4 auth panics on malformed UTF-8 date input | fixed | `./scripts/security-tests parser authn` | Covered by shared auth/parser hardening regressions. |
| `security/codex-14d95e1` Malformed x-amz-date can panic time-skew parsing | fixed | `./scripts/security-tests parser authn` | Covered by SigV4 date parsing regressions. |
| `security/codex-23ffb1b` Copy-source header can crash PG hashing via length assert | fixed | `./scripts/security-tests parser` | Covered by copy-source and query-validation tests. |
| `security/codex-33d8271` POST policy expiration parser can panic on invalid month | fixed | `./scripts/security-tests parser authn` | Covered by POST policy parser hardening. |
| `security/codex-6f7287e` UploadPartCopy skips multipart part-number limit validation | fixed in `fd74f68` | `./scripts/security-tests parser` | Covered by UploadPart query validation. |
| `security/codex-7ceb1e1` Delayed PutObject authorization enables extra body read | fixed | `./scripts/security-tests transport` | Covered by transport/request-handling regressions. |
| `security/codex-7ef29a6` Batch delete skips key validation enabling authenticated DoS | stale | `./scripts/security-tests parser` | The finding is stale; `parse_delete_objects_` coverage now validates DeleteObjects keys. |
| `security/codex-fa165ff` Unbounded multipart field buffering in streaming POST causes DoS | fixed | `./scripts/security-tests parser` | Covered by multipart parsing and body-size regressions. |
| `security/codex-30ada09` Unvalidated response-* overrides can inject response headers | fixed | `./scripts/security-tests transport` | Covered by response override sanitization tests. |
| `security/codex-36005f5` Unvalidated response override headers can panic hyper responses | fixed | `./scripts/security-tests transport` | Covered by response header validation regressions. |
| `security/codex-e190b29` Duplicate Content-Length header in GetObjectPart responses | fixed | `./scripts/security-tests transport` | Covered by `s3_response_to_hyper_` regression tests. |
| `security/codex-195d31f` ListObjectVersions lacks record cap, enabling memory DoS | fixed | `./scripts/security-tests parser` | Covered by server-http and server-core bounded listing regressions. |
| `security/codex-2551b28` Delimiter listing now fetches unbounded rows, enabling DoS | fixed | `./scripts/security-tests parser` | Covered by bounded delimiter-listing regressions. |

## Authentication, Authorization, Ownership, and Bucket Policy

| Finding | Status | Local deterministic path | Notes |
| --- | --- | --- | --- |
| `security/codex-0bd370b` BlockPublicPolicy bypass via full-range SourceIp CIDR | fixed | `./scripts/security-tests authz` | Covered by bucket-policy authz regressions. |
| `security/codex-154e45d` Bucket policy denies skipped for PrincipalArn/SourceVpc clauses | fixed | `./scripts/security-tests authz` | Covered by bucket-policy differential and authz model tests. |
| `security/codex-172cf0a` PutObjectAcl bucket policy ignores ACL/grant conditions | fixed | `./scripts/security-tests authz` | Covered by authz matrix and bucket-policy ACL-condition regressions. |
| `security/codex-6323837` Anonymous owner match grants access to anonymous uploads | resolved | `./scripts/security-tests authz` | Covered by anonymous/public-access regressions. |
| `security/codex-7acfc72` BucketOwnerEnforced does not disable public-read object ACLs | fixed | `./scripts/security-tests authz` | Covered by ownership and ACL enforcement tests. |
| `security/codex-8b367f6` AuthenticatedUsers ACLs bypass BlockPublicAcls enforcement | stale | `./scripts/security-tests authz` | The finding is stale; AuthenticatedUsers and BlockPublicAcls interactions remain covered in authz tests. |
| `security/codex-8f1ed6f` IgnorePublicAcls bypass via AuthenticatedRead object ACLs | invalid | `./scripts/security-tests authz` | Invalid finding; retained here so the existing IgnorePublicAcls coverage stays visible. |
| `security/codex-1bf5cee` Fast-path cache can briefly bypass newly added bucket policy | resolved | `./scripts/security-tests authz` | Resolved by the BOE-only read fast path plus shared generation-tracked freshness and watcher-based stale entry reload/eviction. |
| `security/codex-9e22ede` Unauthorized callers can enumerate bucket names via error codes | invalid | `./scripts/security-tests authz` | Invalid finding; discovery behavior remains covered by authz model and AWS access-matrix tests. |
| `security/codex-a25af39` Constrained same-account users still read/manage data as owners | fixed | `./scripts/security-tests authz` | Covered by ownership and constrained-user matrix tests. |
| `security/codex-bb033e7` Object lock auth order leaks bucket lock configuration | fixed | `./scripts/security-tests authz lifecycle` | Covered by local authz ordering tests and the lifecycle/object-lock group. |
| `security/codex-cece86a` RequestObjectTag policies ignored for PutObjectTagging | fixed | `./scripts/security-tests authz` | Covered by request-tag condition regressions. |
| `security/codex-1b2e520` CreateBucket can drop pending ACL metadata commands | fixed | `cargo nextest run -p storage existing_create_bucket_preserves_pending_acl_command_for_retry` | Covered by a local metadata-command convergence regression; existing CreateBucket no longer clears unrelated pending bucket commands. |

## Stateful Multipart, Reclaim, Lifecycle, and Object Lock

| Finding | Status | Local deterministic path | Notes |
| --- | --- | --- | --- |
| `security/codex-1f0908f` Race in stream segment append can delete committed shards | fixed | `./scripts/security-tests stateful` | Covered by duplicate-append race regressions. |
| `security/codex-470aa8a` Streaming UploadPart reuploads leave orphaned shard data | fixed | `./scripts/security-tests stateful` | Covered by streamed part reupload/orphan cleanup tests. |
| `security/codex-b6ecc5a` Completed multipart upload tombstones accumulate indefinitely | fixed | `./scripts/security-tests stateful` | Covered by multipart cleanup and reclaim regressions. |
| `security/codex-ecef3a4` Unbounded reclaim queue allows memory exhaustion via reads | fixed | `./scripts/security-tests stateful` | Covered by reclaim queue and payload-lease regressions. |
| `security/codex-a09a766` Lifecycle sweep bypasses object-lock retention checks | invalid | `./scripts/security-tests lifecycle` | Invalid finding; retained here so object-lock/lifecycle retention coverage stays visible. |
| `security/codex-cd48ea6` Object Lock headers accept past retention dates | fixed | `./scripts/security-tests lifecycle` | Covered by object-lock retention validation regressions. |

## Observability and Redaction

| Finding | Status | Local deterministic path | Notes |
| --- | --- | --- | --- |
| `security/codex-28f87fb` Control characters now allowed in keys enable log injection | fixed | `./scripts/security-tests redaction` | Covered by debug/log redaction and escaping tests. |
| `security/codex-90f9972` Tracing logs presigned URL queries and credentials | fixed | `./scripts/security-tests redaction` | Covered by redaction regressions for auth and observability surfaces. |
| `security/codex-a413960` SQLite error details now leak in S3 error responses | fixed | `./scripts/security-tests redaction` | Covered by server-http sanitization regressions. |

## Integrity, Checksums, and Low-Level Implementation

| Finding | Status | Local deterministic path | Notes |
| --- | --- | --- | --- |
| `security/codex-2cf85f1` AArch64 PMULL CRC path enabled without PMULL feature check | invalid | `./scripts/security-tests integrity` | Invalid finding; retained here so the low-level integrity surface is still tracked. |
| `security/codex-32d164e` AArch64 CRC fast path lacks PMULL feature gating | invalid | `./scripts/security-tests integrity` | Invalid finding; retained here so the low-level integrity surface is still tracked. |
| `security/codex-6e5f441` POST object ignores SSE-C headers, storing data unencrypted | invalid | `./scripts/security-tests transport integrity` | Invalid finding; SSE-C transport behavior remains covered under local transport tests and AWS oracle suites. |
| `security/codex-ae442f5` PCLMUL CRC loop uses out-of-bounds pointer arithmetic | fixed | `./scripts/security-tests integrity` | Covered by checksum crate regressions. |
| `security/codex-f6004f1` Streaming PUTs without content hash bypass payload integrity | fixed | `./scripts/security-tests parser integrity` | Covered by streaming checksum and parser hardening regressions. |
