# Public API Consistency Review — July 2026

## Context

A workspace-wide review of the public (cross-crate) API surface, prompted by
code churn and a known problem class: parameters that are `Option<T>` but
should be required. The review covered auth, storage, server-core, server-http,
s3-types, checksum, ec, placement, and observability, checking each crate's
`pub` surface against its actual consumers.

Line references are against commit 69470e83.

The review found real bugs, and most sit exactly where an API inconsistency
pointed: duplicate entry points that drifted apart, and `Option` parameters
whose `None` silently disables a check. Findings are grouped as ranked bugs
first, then six recurring design patterns to fix as mechanical sweeps, then
smaller per-crate items.

## Status

Open. Nothing in this plan has been fixed yet.

Suggested order of attack:
1. Auth path gaps (A1-A4) — small, well-localized, each with an existing
   header-path test to mirror.
2. Storage lease deadline (S1). S2 was invalidated by the documented
   no-upgrade policy.
3. Unify checksum-algorithm parsing (K1) and make `from_header_iter` fail
   closed — kills two findings at once.
4. The mechanical sweeps (patterns P1-P6) — churn-proofing that turns the next
   drift into a compile error instead of a review finding.

## Bugs — auth (security)

The three SigV4 entry points (header / presigned / POST) have drifted; each
gap is a hole. The header path is the most complete; the other two are missing
steps it performs.

- [x] **A1. Presigned path does not reject unsigned `x-amz-*` headers.**
  Header path enforces via `unsigned_required_headers()`
  (`crates/auth/src/sigv4.rs:101-125`) that `host` and every `x-amz-*` header
  present are signed. `authenticate_presigned`
  (`crates/auth/src/request.rs:466-470`) checks only `host`. A valid presigned
  URL plus an unsigned `x-amz-acl: public-read`, `x-amz-tagging`, or
  `x-amz-meta-*` header passes auth and server-http honors those headers. The
  same relaxation lets `x-amz-security-token` arrive as an unsigned header
  (`request.rs:459-463`) where the header path requires it signed (test
  `authenticate_header_unsigned_security_token_rejected`). Fix: call
  `unsigned_required_headers` in `authenticate_presigned`; accept the session
  token only from the (signed) query parameter. Mirror test
  `verify_request_unsigned_amz_header` (sigv4.rs:717) for the presigned path.
  Completed with AWS oracle coverage for unsigned `x-amz-meta-*`,
  `x-amz-acl`, and `x-amz-security-token` presigned requests. AWS returns
  `HeadersNotSigned` for those, but returns `SignatureDoesNotMatch` for an
  unsigned `x-amz-content-sha256` header, so the implementation preserves that
  presigned-specific exception.

- [x] **A2. POST SigV4 skips credential-expiry and security-token rejection
  entirely.** `authenticate_post_sigv4` (`crates/auth/src/post.rs:56-124`)
  did not validate credential expiry, did not receive a timestamp, and did not
  reject a supplied POST Object security-token form field. Header and presigned
  auth already apply expiry validation and reject token inputs for Argmin's
  supported static credentials. The sole call site
  (`crates/server-http/src/http/mod.rs:3430-3446`) does not compensate. Fix:
  add `now_epoch_secs` and the security-token form field to
  `authenticate_post_sigv4` and run the same static-credential validation.
  Completed with AWS oracle coverage for the static-credential POST Object
  case: an unexpected `x-amz-security-token` form field returns
  `InvalidToken`. Argmin does not implement STS or temporary session
  credentials, so stored session-token support was removed from
  `CredentialRecord`; only credential-expiry checks remain as a local
  credential property.

- [x] **A3. `now_epoch_secs == 0` is a sentinel that disables expiry checks,
  and production can produce it.** `request.rs:557-559` skipped credential
  expiry when `now == 0`, and presigned expiry (request.rs:442) also did not
  fire for epoch-dated requests if the server timestamp had defaulted to `0`.
  server-http computed now with
  `SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default()`
  (`crates/server-http/src/http/mod.rs:3086-3090`) — a broken clock yields
  exactly the sentinel. Completed by treating `0` as a normal Unix-epoch
  timestamp in auth validation and making server-http auth timestamp
  acquisition fail closed instead of defaulting clock errors to `0`. Added AWS
  oracle coverage proving an epoch-dated presigned URL is rejected rather than
  accepted.

- [x] **A4. Presigned path has no future-skew check.** Header auth rejects
  `|now - x-amz-date| > 900s` both directions (request.rs:283-288); presigned
  checked only expiry (request.rs:429-444), so an `X-Amz-Date` in the future
  was accepted immediately and stayed valid until then + expires. AWS oracle:
  a presigned URL dated `21000101T000000Z` returns 403 AccessDenied with
  message `Request is not yet valid`. Completed by rejecting
  `request_epoch > now + SIGV4_CLOCK_SKEW_SECS` in `authenticate_presigned`
  with a presigned-specific auth error that renders the AWS response shape.

- [x] **A5. One `AuthError::RequestExpired` conflates two AWS errors.**
  Produced for header clock skew (request.rs:287) and presigned expiry
  (request.rs:443); mapped unconditionally to `RequestTimeTooSkewed`
  (`crates/server-core/src/error.rs:389`). AWS oracle: an epoch-dated
  presigned URL returns 403 AccessDenied with message `Request has expired`,
  `RequestId` and `HostId`, and no `Resource` element, while header skew
  remains `RequestTimeTooSkewed`. Completed by splitting presigned expiry into
  `PresignedRequestExpired`, mapping it to `AccessDenied`, and routing it
  through the AccessDenied-shaped response formatter.

- [x] **A6. Presigned body-hash selection uses unsigned header values.**
  AWS oracle: an unsigned `x-amz-content-sha256: UNSIGNED-PAYLOAD` header on a
  URL signed with `UNSIGNED-PAYLOAD` is accepted, while an unsigned real
  payload hash on that same shape returns `SignatureDoesNotMatch` rather than
  `HeadersNotSigned`. The existing request path already matches this
  value-sensitive behavior by excluding `x-amz-content-sha256` from generic
  unsigned-header rejection and using its value in canonical request
  verification when present. Completed by tightening AWS-facing s3-tests; no
  auth code change is needed.

- [x] **A7. POST auth reports `AuthMode::HeaderSigV4`.** Downgraded from an
  AWS-visible auth bug to an internal API-correctness issue: current
  production consumers only distinguish anonymous from authenticated POST auth
  and do not branch on `HeaderSigV4`, so no downstream AWS behavior divergence
  was identified. Completed by adding `AuthMode::PostSigV4` and returning it
  from POST authentication so future downstream matches cannot accidentally
  treat POST Object auth as canonical header auth.

- [x] **A8. Region/scope mismatch yields path-specific AWS errors.** AWS
  oracle confirms the three SigV4 surfaces are intentionally different:
  header auth wrong region/service returns 400 `AuthorizationHeaderMalformed`,
  presigned wrong region/service returns 400
  `AuthorizationQueryParametersError`, and POST Object wrong region/service
  returns 400 `InvalidArgument`. Completed by adding AWS-facing presigned and
  POST Object scope mismatch tests and by making deferred bucket-region
  validation preserve the auth path's AWS error family instead of converting
  every late region mismatch into `AuthorizationHeaderMalformed`.

- [x] **A9. Clock-skew rule implemented twice.** AWS oracle confirms header
  SigV4 stale past/future dates return 403 `RequestTimeTooSkewed`, and stale
  tampered `x-amz-date` is rejected for skew before signature mismatch.
  Completed by deleting the server-http pre-auth copy and keeping
  `auth::authenticate_request` as the single header-auth skew enforcement
  path.

- [x] **A10. POST policy parser silently drops malformed/unknown conditions.**
  post.rs:202-268 — array conditions with wrong arity and unknown operators
  are ignored (tests codify this); AWS rejects invalid policy documents. A
  signed constraint like a short `["starts-with","$key"]` vanishes without
  error. Completed by adding AWS-facing POST Object oracle tests for wrong
  arity `starts-with`, `eq`, and `content-length-range` conditions, unknown
  operators, and scalar conditions. AWS returns `400 InvalidPolicyDocument`
  with `RequestId` and `HostId` and no `Resource`, so the parser now rejects
  those condition forms and the HTTP layer renders the AWS-shaped error family.

## Bugs — storage

- [x] **S1. Fixed: bucket write reservations acquired with `lease_deadline: None`
  were immortal and could permanently wedge DeleteBucket.** The old general
  acquire paths passed `None` (`crates/storage/src/cluster/request_ops.rs:2642-2683`,
  also 2550/2602); only stream-create passed `Some` (request_ops.rs:2713).
  Validation only expired a reservation `if lease_deadline.is_some_and(...)`
  (`node_client/local.rs:1798-1806`). There was no reaper for reservation rows
  (contrast `clear_expired_durable_bucket_write_drain`,
  `pg_store/metadata.rs:5828-5919`) and no startup/recovery cleanup.
  `DurableBucketWriteReservation` has no `Drop` releasing the row. DeleteBucket
  waited for the reservation list to empty (`request_ops.rs:3224-3299`); an
  orphaned `None`-lease row (crash or panic unwind between acquire and release)
  made every DeleteBucket for that bucket fail forever. This contradicted
  `guides/bucket-write-drain.md` ("Each reservation must include ... lease
  deadline"; "Release, reap, and apply-time validation").

  Resolution: bucket write reservation records and proofs now carry a required
  `lease_deadline: u64`, the SQLite schema rejects `NULL` deadlines for
  `bucket_write_reservations`, the local/Unix/RPC acquire paths require a
  concrete deadline, proof matching includes the deadline, and DeleteBucket
  reservation wait releases exact expired reservation rows before deciding the
  bucket is blocked. Added a local-cluster regression covering an unreleased
  expired durable reservation being reaped during DeleteBucket.

- [x] **S2. Invalid: new `buckets` column added without a migration.** This
  finding assumed pre-alpha stores are upgraded in place. They are not:
  `plans/storage-upgrade-versioning-plan.md` documents the explicit no-upgrade
  policy, and `guides/threat_model.md` states older database schemas are not
  supported until a future stability point. Do not add idempotent `ALTER`s for
  this. Existing speculative baseline migrations should be audited and removed
  under Phase 0 of the storage upgrade/versioning plan.

- [ ] **S3. `CompleteReadyPgPeerings` skips the node-service authorization
  its single-item twin enforces.** `CompletePgPeering` carries
  `node_incarnation` and calls `authorize_node_service_for_snapshot`
  (`control_plane.rs:1903-1924`); the batch apply (control_plane.rs:2010-2126)
  never does, and `ReadyPgPeeringCompletion`
  (`control_plane_command.rs:160-165`) has no incarnation field — a node
  restart between proposal (control_plane.rs:3494-3508) and raft commit is not
  fenced. Replay idempotency is also asymmetric: single variant tolerates
  already-Active-same-primary; batch rejects with `PgNotPeering`. Fix: add
  `node_incarnation` per completion and authorize (bump command version), or
  document why leader-derived batches deliberately skip it; align replay
  behavior.

- [ ] **S4. Tautological `matches_request` argument neutralizes the
  generation check.** `cluster.rs:553-555` passes
  `commit.object.generation_id` as the generation argument to
  `commit.matches_request(...)`, making the comparison in
  `metadata_command.rs:697-709` vacuously true. Sibling caller
  `node_client.rs:659` passes a real request generation. Fix: split into
  `matches_session(...)` and `matches_request(..., GenerationId)` so
  "don't-care" must be explicit.

- [ ] **S5. MPU-cleanup resume cursor is a positional index, not a PG id.**
  `delete_completed_multipart_uploads_for_bucket`
  (`cluster/request_ops.rs:~5953`, commit 69470e83) persists `next_pg_index`
  into the call-time-sorted `metadata_pg_ids()`. The current implementation
  sorts the PG list and has sparse-PG regression coverage, so this is not a
  current bug while the configured PG set is fixed for the cluster lifetime.
  It is a real topology-resize hazard: if the metadata PG set changes between
  crash and resume, the index silently re-targets different PGs, skipping
  cleanup on some. Track this under
  `plans/storage-topology-resize-plan.md` H5. A resize-safe fix should persist
  a semantic cursor such as last-completed `PgId` plus topology generation, or
  make the cleanup phase generation-scoped and restartable from zero when the
  PG set changes.

- [ ] **S6. Route-map validity is a storage routing freshness contract, not a
  generic option cleanup.** `route_map_valid_until_ms: Option<u64>` uses
  `None` to mean "valid forever" in both storage-node serving config
  (`storage/src/storage_node_server.rs:322, 451-463`) and frontend/local
  cluster maps (`storage/src/cluster/local.rs:1869-1905`). This affects
  distributed-storage safety rather than AWS-visible request semantics:
  metadata primary/replica routing, metadata command acceptance, payload
  placement, shard IO, bucket-delete begin/finalize loops, and storage-node
  RPC route validation all use the deadline to stop trusting stale routes.
  Control-plane runtime maps set the deadline from the minimum active primary
  lease deadline (`storage/src/control_plane.rs:701-704`), while static/local
  topology paths intentionally construct unbounded maps (for example
  `argmin-s3/src/main.rs:2950-2954`). Fix: replace the implicit `Option` with
  an explicit `RouteMapValidity::{Forever, Until(u64)}` and then constrain
  `Forever` to the static/local topology constructors or other deliberately
  unbounded modes.

## Bugs — checksum handling (cross-crate)

- [x] **K1. Checksum algorithm headers were modeled against the wrong wire
  header.** AWS-pinned tests showed that PutObject uses
  `x-amz-sdk-checksum-algorithm` to declare the SDK-selected algorithm and
  requires a corresponding concrete `x-amz-checksum-*` value header or
  `x-amz-trailer`. A mismatched or invalid SDK header returns 400
  `InvalidRequest` with HostId/no Resource XML; lowercase SDK values are
  accepted. The literal `x-amz-checksum-algorithm` is ignored for PutObject
  metadata/value matching and must not affect stored checksum metadata, but is
  real for CreateMultipartUpload and CopyObject replacement checksum selection,
  where lowercase values are accepted and unsupported values return AWS's 400
  `InvalidRequest` unsupported-algorithm message. Fixed in
  `crates/server-http/src/http/mod.rs` and response shaping in
  `crates/server-http/src/http/response.rs`, with s3-test coverage in
  `crates/s3-tests/tests/checksums.rs`.

- [x] **K2. Multipart checksum create-algorithm requirement needed AWS
  clarification.** AWS-pinned tests showed the UploadPart
  `(None, Some(part_algo))` path is intentional: a new-algorithm part checksum
  can be accepted even when CreateMultipartUpload omitted a checksum algorithm,
  but CompleteMultipartUpload does not store it and part checksum elements fail
  as `InvalidPart`. CompleteMultipartUpload object checksum headers are
  family-sensitive: legacy `CRC32`/`CRC32C`/`SHA1`/`SHA256` headers are accepted
  but ignored and not stored when Create omitted the algorithm, while new
  `MD5`/`SHA512`/`XXHASH64`/`XXHASH3`/`XXHASH128` headers return AWS-shaped
  400 `InvalidRequest` with message `Checksum Type mismatch occurred, expected
  checksum Type: null, actual checksum Type: <algo>`. Fixed local error text
  and response shape in `crates/server-core/src/coordinator/multipart.rs` and
  `crates/server-http/src/http/response.rs`; expanded s3-test coverage in
  `crates/s3-tests/tests/checksums.rs`.

- [x] **K3. `CHECKSUM_HEADERS` in server-http hand-duplicates
  `ChecksumAlgorithm::ALL` + `header_name()`**
  (`crates/server-http/src/http/mod.rs:4918-4929`, with
  `parse(algo).expect(...)` at mod.rs:5459). A new algorithm in the checksum
  crate would silently not be recognized. Also `validate_checksum_headers`
  (mod.rs:5431-5487) re-implements base64/length validation that
  `ChecksumClaim::from_base64` owns and, unlike sibling
  `extract_encoded_checksum_header` (mod.rs:5500-5539), does not reject
  duplicate same-name checksum headers. Fix: build the table from
  `ChecksumAlgorithm::ALL`; have `validate_checksum_headers` delegate so both
  paths share duplicate-header and format rules. Fixed
  `crates/server-http/src/http/mod.rs` to derive checksum headers from
  `ChecksumAlgorithm::ALL`, remove the string-to-enum parse/expect paths, and
  route `validate_checksum_headers` through `extract_encoded_checksum_header`
  plus `ChecksumClaim::from_base64`. Added local coverage for all algorithms
  and duplicate same-name checksum headers.

- [x] **K4. Multipart complete path split the validated
  `MultipartChecksumConfig` back into two independent `Option`s.** Fixed
  `crates/server-core/src/coordinator/multipart.rs` to carry the validated
  config through CompleteMultipartUpload and compute the checksum from that
  config directly. The previous impossible invalid-combination `InternalError`
  arm is now an unreachable `MultipartChecksumConfig` invariant inside the
  checksum computation helper.

## Bugs — server layer

- [x] **H1. Internal failures returned as 400 `InvalidRequest` instead of
  500.** `internal_error_response` (`crates/server-http/src/http/serve.rs:4874`,
  copy-pasted as closures at serve.rs:2632 and serve.rs:2979) wraps
  `ServerError::InvalidRequest { reason: "internal error" }` → HTTP 400. The
  blocking-handler-panic path (serve.rs:939) hits this, so genuine server
  faults are invisible to 5xx alerting and the `panic_on_500`/`abort_on_500`
  hooks. Fixed `internal_error_response` to use `ServerError::InternalError`
  and routed the streaming POST, streaming PUT, and streaming UploadPart
  join-error paths through the shared helper. Added local response coverage for
  the helper to pin 500 `InternalError` and the generic internal-error message;
  this is not an AWS oracle case because it covers server-internal panics/join
  failures rather than a client-reachable S3 semantic.

- [x] **H2. `If-Match` against a missing object returns 412; AWS returns 404
  for conditional writes/deletes on nonexistent objects.** The original finding
  missed existing public `s3-tests` coverage for write paths:
  `test_put_object_ifmatch_nonexisted_failed` and
  `test_complete_multipart_ifmatch_nonexisted_failed` already pin 404
  `NoSuchKey` locally and against AWS. The real gap was `DeleteObject` with
  `If-Match` on a missing current object: added
  `test_delete_object_ifmatch_nonexistent_returns_no_such_key`,
  `test_delete_object_ifmatch_versioned_nonexistent_returns_no_such_key`, and
  `test_delete_object_ifmatch_current_delete_marker_returns_no_such_key`,
  verified them against AWS, and fixed both conditional delete paths in
  `coordinator/delete.rs` to return `ObjectNotFound` when the current object is
  absent/non-live while preserving 412 for ETag mismatch on a live object. The
  versioned tests use the shared versioning helper that waits for versioned
  writes to converge. The lower-level
  `check_write_conditions(None)` unit remains an internal helper behavior;
  public write callers convert `If-Match` with no existing object to
  `ObjectNotFound` before calling it.

- [x] **H3. Lifecycle `render_filter` dead guard: empty `<Filter/>` renders
  as `<Filter><And></And></Filter>`.** `crates/s3-types/src/lifecycle.rs:264`
  — the condition `!filter.has_scope() || filter.explicit_filter &&
  !filter.has_scope()` is provably always false at that point (has_scope()
  returns true whenever explicit_filter is true; the branch is only reachable
  when explicit_filter || tags || size-filter holds). Since PutBucketLifecycle
  stores the rendered canonical XML (`server-http/src/http/mod.rs:2231`), a
  client PUTting `<Filter/>` gets the non-canonical `<And></And>` form back
  from GetBucketLifecycleConfiguration — a form AWS never emits. Added
  `test_bucket_lifecycle_raw_get_empty_filter_returns_canonical_xml` and
  verified against AWS that the canonical response preserves the self-closing
  `<Filter/>` shape. Fixed `render_filter` to check for absence of concrete
  predicates (`prefix.is_none() && tags.is_empty() && !has_size_filter()`) and
  emit `<Filter/>`; added local renderer coverage in `s3-types`.

- [x] **H4. `NewerNoncurrentVersions` validation predicate contradicts its
  error message.** `lifecycle.rs:413-417` — message says "requires an explicit
  lifecycle filter" but the predicate `!rule.filter.has_scope()` also passes a
  legacy `<Prefix>` rule. AWS-pinned the ambiguous cases: explicit
  `<Filter><Prefix>...</Prefix></Filter>` and empty `<Filter/>` with
  `NewerNoncurrentVersions` are accepted, while legacy top-level `<Prefix>` with
  `NewerNoncurrentVersions` is rejected as `InvalidRequest` with message
  `NewerNoncurrentVersions element can only be used in Lifecycle V2.` Updated
  validation to preserve the existing no-filter `MalformedXML` path while
  rejecting the legacy-prefix V1 form with the AWS-shaped error, including the
  AWS response XML shape (`RequestId` and `HostId`, no `Resource`).

- [x] **H5. Quoted-star `If-Match` semantics disagree between core and HTTP
  layers.** Core docs + test say `If-Match: "*"` is a specific etag → 412
  (`server-core/src/conditional.rs:31-35, 562-568`); the HTTP layer returns
  501 `NotImplemented` (`server-http/src/http/conditional.rs:62-67`, test at
  161-165). The two test suites pin contradictory semantics; one layer's
  behavior is dead code. This finding was stale/misframed for public behavior:
  `s3-tests` already pin AWS `PutObject If-Match: "\"*\""` as 501, and
  `./scripts/aws-tests --test conditional ifmatch -- --nocapture` confirmed the
  full conditional `ifmatch` slice. Added AWS-pinned CopyObject destination
  coverage for quoted-star `If-Match`, which also returns 501. The core parser
  still correctly treats quoted star as a literal ETag token for read/delete
  matching, so the misleading core write-condition unit was rewritten to test
  `EtagMatchList` literal-token behavior instead of an impossible public write
  condition.

- [x] **H6. Two different 416 bodies, and `total_size` computed then
  dropped.** `range_not_satisfiable{,_with_ids}` ignores its `_total_size`
  parameter (`server-http/src/http/response.rs:1209-1235`); the GET/HEAD
  partNumber path maps `InvalidPart` → `InvalidRange { total_size: 0 }` →
  generic `error_xml` (mod.rs:1687-1692, 1866-1871; response.rs:267, 715-722);
  core carefully computes `InvalidRange { total_size: record.size }`
  (read.rs:869-872) that is never rendered. AWS includes `<ActualObjectSize>`.
  Fix: one 416 builder rendering ActualObjectSize from total_size. AWS-pinned
  the details first: unsatisfiable byte `Range` returns `InvalidRange` with
  `<ActualObjectSize>`, `RequestId`, and `HostId` and no `Resource`, including
  for zero-byte objects. The `partNumber` branch is distinct from the original
  diagnosis: AWS returns 416 `InvalidPartNumber` with
  `<PartNumberRequested>` and `<ActualPartCount>`, not `InvalidRange`. Added a
  read-specific `InvalidPartNumber` error and AWS-shaped response while leaving
  CompleteMultipartUpload `InvalidPart` semantics unchanged.

- [x] **H7. SSE-C validator-key-unavailable reported as client 400.**
  `server-core/src/sse.rs:495-499` returns `InvalidRequest` when the server no
  longer holds the validator key (rotation/operational fault); genuine wrong
  key correctly gets `AccessDenied` (sse.rs:502). Write-resume path
  (sse.rs:685-702) maps the same condition to a different 400 message. Fix:
  map key-unavailable to a 500-class error; unify read/write messages. This is
  not AWS-oracle testable because it requires stored SSE-C metadata whose
  validator key id is no longer configured locally. Added unit coverage for
  both read and write-resume paths; wrong customer keys remain client errors.

- [x] **H8. Copy-source `versionId` never percent-decoded, unlike bucket and
  key.** `server-http/src/http/request.rs:387-416` (pinned by test at
  535-541), consumed raw at mod.rs:234-240. Fix: decode with the same
  `percent_decode_strict`. AWS-pinned first with CopyObject against a versioned
  source where the first byte of `versionId` is percent-encoded in
  `x-amz-copy-source`; AWS decodes it and copies that exact version. Local
  parser now strictly percent-decodes copy-source `versionId`.

## Pattern sweeps

The recurring shapes behind the individual findings. Fix as workspace-wide
sweeps so the next drift is a compile error.

### P1. `Option` parameters where `None` silently weakens semantics

- [x] Auth scope APIs used `Option` to mean "skip region validation":
  `authenticate_request` and `ExpectedCredentialScope::new` both treated
  `None` as no scope check. Completed by introducing
  `ExpectedSigningRegion::{ExactEndpointRegion, DeferredToBucketRouting}` and
  using that type from both header/presigned request auth and POST Object auth.
  This keeps S3 bucket-routing policy in server-http while making the auth API
  explicit.
- [x] Deferred bucket-region validation is AWS-visible and separate from the
  auth API shape: server-http passes deferred scope for bucket-named
  operations, then relies on `enforce_bucket_region`, which returns `Ok(())`
  for missing buckets (`server-http/src/http/mod.rs:3177, 3191-3209`). A8
  already pinned existing-bucket wrong-region behavior for header, presigned,
  and POST auth. Completed by adding account-regional missing-bucket AWS oracle
  coverage for header and presigned auth: AWS still validates the
  credential-scope region before bucket lookup, returning the auth-family
  wrong-region error rather than `NoSuchBucket`. POST Object is not on the
  deferred request-auth path; it still validates against the endpoint signing
  region during POST authentication.
- [x] Observability formerly had `timeout_us: Option<u128>` on
  `RequestAdmissionSummary`/`StorageRpcAdmissionSummary`: every workspace
  caller passed `Some`; on timeout emitters `None` rendered "timed out after 0us" via
  `unwrap_or_default()` (lib.rs:2264, 2279, 3222, 3235). Completed by
  splitting each admission summary into wait and timeout variants. Wait
  events now have no timeout field, while timeout events require a plain
  `timeout_us: u128`.
- [x] `begin_bucket_delete` (unguarded, `Option<(u64,u64)>` identity) vs
  `begin_bucket_delete_if_current` (`cluster/request_ops.rs:3947, 3951`):
  production only used the guarded one; `None` identity was a logic error for
  production callers. Completed by removing the unguarded variant, making the
  guarded API take a named `BucketIdentityGenerations` struct, and switching
  tests to either call a test helper that reads the current identity then calls
  the guarded API, or pass an explicit saved identity in stale-retry cases.
- [x] Bucket policy: `InputUnavailable` is an AWS-semantics state, not
  itself a correctness bug. The evaluator intentionally treats unavailable
  inputs as nonoperative for both Allow and Deny so that accepted-but-not-
  evaluable conditions stay AWS-compatible, for example
  `s3:ExistingObjectTag/*` on `GetObjectAttributes`. The remaining P1 risk is
  production wiring: a caller can still build a `PolicyRequest` with an
  unavailable input even when the parsed policy declares that input is required
  for this action. Current server-core callers mostly compensate by
  pre-consulting `requires_*_for_action`, and the modern BOE bucket-tag path
  already returns an internal error if required preloaded tags are missing, but
  this convention is not uniformly type-enforced. Fix: keep evaluator semantics
  unchanged; make production request builders return a controlled internal
  error when `policy.requires_*_for_action(action)` is true but the supplied
  input is unavailable, add debug assertions at the invariant boundary, and add
  mechanical coverage for every legacy/modern call path plus the intentional
  accepted-but-not-evaluable cases.

### P2. Duplicate entry points that drifted

(A1-A9 above are the worst case. Remaining instances:)

- [x] Two reservation-release APIs with opposite not-found semantics:
  `release_durable_bucket_write_reservation` errors on 0 rows
  (`pg_store/metadata.rs:5606-5610`);
  `release_metadata_command_bucket_write_reservation` returns `Ok`
  (metadata.rs:5683-5685). Resolution: keep the split because the durable
  release is a live owner release where missing rows indicate stale/conflicting
  ownership, while metadata-command release is terminal command cleanup that
  must be idempotent under replay/recovery. The trait docs now state both
  contracts, store and Unix RPC tests pin the idempotent metadata-command
  release path, and `BucketWriteReservationProof::matches_record` documents
  that it deliberately skips mutable `lease_deadline` because freshness is
  checked separately from proof identity.
- [x] `SystemMetadata::from_headers` / `from_pairs` are byte-identical
  (`system_metadata.rs:114-116, 186-188`) while `MetadataBlob`'s same-named
  pair differ materially: `MetadataBlob::from_pairs` skips the
  `has_invalid_header_bytes` header-injection guard and Latin-1
  reinterpretation that `from_headers` performs, its doc is wrong (claims no
  lowercasing; code lowercases), and `set()` enforces the key rule with
  `assert!` where `from_pairs` returns `Err`
  (`metadata_blob.rs:76-89, 263-297`). Resolution: remove the redundant
  `SystemMetadata::from_pairs` alias and the divergent `MetadataBlob::from_pairs`
  / unused `set` APIs. Existing tests now use the production `from_headers`
  constructors, so request-shape metadata parsing has a single validation path.
- [x] `MultipartObjectRequest`: `new`/`new_typed`, `from_object`/
  `from_object_typed`, `upload_id`/`upload_id_typed` are byte-identical
  duplicates that muddy the `_typed` convention
  (`coordinator/request_types.rs:1028-1093`). Resolution: delete the aliases
  and update call sites to use `new`, `from_object`, and `upload_id`, which
  already take and return typed `UploadId` values.
- [x] Dead SSE-S3 alias layer: `SseS3WriteContext` + four `sse_s3_*` fns only
  tests call (`sse.rs:379, 643-683`). Migrate tests to `managed_encryption`
  names, delete. Resolution: remove the alias type and wrapper functions, and
  update the SSE-S3 behavior tests to call the managed-encryption helpers
  directly.
- [x] Control-plane `_checked` siblings exist for the acting-set mutators but
  the epoch-only Unix `fence_pg_for_metadata_transfer` path had no idempotent
  retry story. Resolution: keep the existing checked runtime-map fence as the
  supported Unix client API, remove the unsafe epoch-only Unix client/RPC
  surface, and make the raw source-lease runtime-map helper internal to the
  checked wrapper. The in-process authority fence remains as the operation
  executed by the checked runtime-map RPC.
- [ ] auth: `pub verify_request` (sigv4.rs:156-170, re-exported lib.rs:75) is
  an attractive-but-incomplete verification door (no skew/expiry/token/scope)
  with zero external callers. Demote to `pub(crate)`.
- [ ] auth: private, dead `combine_decisions` (deny-wins collapse,
  `bucket_policy/evaluator.rs:137-151`) while server-core hand-rolls the
  three-way match at `authz/policy.rs:499-519` and `authz/modern.rs:889-931`.
  Export it and migrate consumers.

### P3. Validating constructors bypassed by public fields

No live bug in any of these (all current callers use the constructors), but
each is one refactor away from a panic:

- [ ] `EcConfig` pub fields (`ec/src/codec.rs:13-18`): `k=0` panics in
  encode/verify/reconstruct (codec.rs:332, 416, 568); `k+m > 32` overruns the
  fixed 32×32 stack matrices in `reconstruct_shards`
  (`ec/src/reconstruct.rs:18-26`). `ErasureCodec::new` returns `Result` but
  has no failure path (codec.rs:254-284) — validating the config there makes
  the `Result` honest and closes the hole.
- [ ] `PlacementConfig.total_shards` pub field (`placement/src/config.rs:7`):
  bypasses `new()` validation; `Placer::place` then writes past `MAX_SHARDS`
  stack arrays (placer.rs:133, 206) — out-of-bounds panic in the hot path.
  Make the field private (also: `new(total_shards: u8)` forces a lossy
  `as u8` at the consumer, local.rs:4425 — take usize and validate).
- [ ] `StorageNodeProcessConfig`: 9 pub fields whose consistency is enforced
  only by the separate `validate_storage_node_process_configs`
  (`storage_node_server.rs:318, 803`). Private fields + validating
  constructor.
- [ ] `SseCustomerRequest::with_algorithm` accepts any string, breaking the
  AES256 pin that `new()` establishes (`sse.rs:54-57`); validation lives only
  in server-http. Drop it or make it fallible.
- [ ] `WriteEncryptionRequest::from_request_parts` has
  `unreachable!` on conflicting SSE-C+SSE-S3 inputs
  (`coordinator/request_types.rs:317-329`) — invariant enforced in a
  different crate, 5 call sites; with `abort_on_500` a future mistake is a
  process abort. Return `Err(InvalidArgument)`.
- [ ] `LifecycleRuleFilter` pub fields permit states the parser never
  produces (legacy rule with tags) which render as V2 `<Filter>`. Constructor
  or doc note on `explicit_filter`.

### P4. Positional same-typed parameters

- [ ] Claim acquire fns take 3 consecutive `u64` timestamps + 2 `&str` tokens
  (`cluster.rs:8788 acquire_placed_segment_shard_repair_claim`,
  `cluster.rs:9149 acquire_next_placed_segment_shard_backfill_claim`);
  consumers pass `now_ms` in two of the three slots
  (`server-core/src/coordinator/runtime.rs:1376, 1684`). The params structs
  already exist (`types.rs:1593 PlacedSegmentShardRepairClaimAcquire`,
  `types.rs:1638`) — use them in the signatures.
- [ ] `acquire_durable_bucket_write_reservation`: 9 positional params
  repeated across four layers (`traits.rs:96-107`, `node_client/local.rs:
  1749-1760`, interface, `pg_store/metadata.rs:5372-5382`); the same trait's
  heartbeat API already uses a params struct. Introduce the acquire struct
  (natural place to make `lease_deadline` required per S1).
- [ ] `PgMetadataProof::new(u64, u64, u64)` (`control_plane.rs:2859`) —
  index/hash/digest transposition compiles silently and poisons peering-proof
  comparison. Named-field construction only, or newtypes.
- [ ] `TraceContext::from_ids(String, String)` (`observability/src/lib.rs:39`)
  — trace/request id swap risk at wire boundaries.
- [ ] `put_bucket_acl_and_load_info(..., public_read: bool, public_write:
  bool)` (`request_ops.rs:6460`) and `with_rpc_admission(Duration, Duration)`
  (`cluster/local.rs:154-160`) — adjacent same-typed pairs; low priority (no
  bare-literal call sites today).
- [ ] Metrics render site pairs ~115 name strings positionally with ~115
  `u64`s (`server-http/src/http/serve.rs:1474-1620`); tests check presence,
  not values, so a transposition mislabels metrics silently.
  `bucket_lock_wait_exceeded_total` is deliberately excluded from export
  (test serve.rs:5202) with no documentation why. Fix: observability exposes
  `MetricsSnapshot::iter_named() -> impl Iterator<Item = (&'static str, u64)>`
  (or a macro generating struct + names together).

### P5. Canonical tokens duplicated across crates / stringly-typed dispatch

- [ ] Observability dimension keys are `&'static str` matched with silent
  `_ => {}` fallthrough (`lib.rs:2284-2315, 1748-1759`): stream-upload
  `phase`, storage-rpc `admission_class`, background-work `event`. Live
  instance: server-http emits phase `"finalize_started"` which matches no arm
  — counter silently never updates. Fix: `enum StreamUploadPhase` /
  `StorageRpcAdmissionClass` / `BackgroundWorkEvent` in observability (storage
  already has a private class enum mirroring the list — move it), match
  exhaustively.
- [ ] `VersionId` has `Display` in s3-types but its inverse parser lives in
  server-http and is lossier (`mod.rs:128-137` accepts `versionId=0` as the
  null version, which Display never produces). Fix: `impl FromStr for
  VersionId` in s3-types accepting `"null"` and `>= 1` only.
- [ ] `BucketNamespace::as_header_value` has no parse counterpart —
  server-http matches `"global"`/`"account-regional"` literals
  (mod.rs:509-510 vs s3-types lib.rs:250-255). `BucketVersioningState` has
  `from_u8` but no `as_str`/`parse`; `"Enabled"`/`"Suspended"` are literals in
  `xml.rs:1441-1442`. Add the missing halves.
- [ ] auth helper triplication: `hex_encode` ×3, `percent_decode` ×2,
  `hex_val` ×2 across sigv4.rs/canonical.rs/request.rs; and
  `server-core/src/sse.rs:779` re-implements `constant_time_eq` verbatim
  instead of using `auth::constant_time_eq`. Consolidate.

### P6. Error-type islands and wrong-blame mappings

- [ ] Stringly-typed errors in otherwise fully-typed crates:
  `AclGrants::parse -> Result<_, String>` (`s3-types/src/lib.rs:843`, both
  consumers immediately wrap it); `PgTopology::new -> Result<_, &'static str>`
  (`pg_topology.rs:57`, consumers `.unwrap()`/`.expect()` it);
  `SseCustomerObjectState::decode`/`SseS3ObjectState::decode ->
  Result<_, String>` (`storage/src/types.rs:948, 1073`); `validate ->
  Result<(), &'static str>` (types.rs:3067); `RawChecksum::new`/
  `ChecksumBytes::new -> Result<_, &'static str>` (`checksum/src/types.rs:298,
  340`, callers all discard the message);
  `SseCustomerValidatorConfig::from_base64`/`ManagedWrappingKeyConfig::
  from_base64 -> Result<_, String>` (`sse.rs:112, 147`). Fix: small typed
  error enums throughout.
- [ ] `parse_bucket_policy` doesn't enforce its own exported
  `MAX_BUCKET_POLICY_BYTES` (`bucket_policy.rs:9, 1170`); only server-core
  enforces it, against the normalized output not the raw input
  (`authz/bucket.rs:545`). Check length first in `parse_bucket_policy`.

## Smaller items

### Placement / determinism

- [ ] **No cross-version placement pin.** Placement is recomputed at read
  time (`storage/src/cluster.rs:8146, 9257, 9768, 10507`); `rapidhash = "4"`
  floats on semver; any drift in hash/Unit53/log tables silently strands every
  existing shard. Fix: golden-vector test (fixed cluster + keys → expected
  NodeId sequences, committed) in the placement crate.
- [ ] **Deterministic-log tables have no checked-in generator.** The 30-row
  corpus touches ≤30 of 363 fast-path table indices; a corrupted
  `INVERSE[i]`/`LOG_INV[i]` pair yields silently wrong but self-consistent
  scores. (Behaviorally verified healthy today: 2M random inputs within 1 ulp
  of `f64::ln`, monotonic.) Fix: check in the generator or a build-time check;
  extend the corpus to cover every table index (~400 rows, mechanical).
- [ ] `rack_cap` doc says "default max = m" but m=0 makes every placement
  fail with `ConstraintUnsatisfiable` (`constraint.rs:76-80`,
  demonstrated by placer.rs:490-512). Document `max >= 1` or reject 0.
- [ ] Documented zero-alloc contract is false for keys > 516 bytes
  (`hash.rs:26-29` heap fallback per node per call; zero-alloc tests only use
  short keys). Document the limit at `place()` or pre-hash long keys.
- [ ] Module-visibility mix (dual paths to every type except `Placer`,
  lib.rs:1-13); `MAX_SHARDS` load-bearing but `pub(crate)` (lib.rs:17);
  `PlacementError` variants carry raw `u32` despite `NodeId` existing
  (config.rs:28-31).

### Observability

- [ ] Raw inc/dec counter pairs are pub where RAII guards should be:
  `emit_background_work_admission_event` wraps the stored atomic to
  `u64::MAX` on unpaired `"finished"` (`lib.rs:3095-3108`; same for
  `storage_rpc_admission_class_acquired/_released`, lib.rs:1703-1715);
  correctness currently rests on consumer `Drop` impls. Expose guards (as
  already done for `inflight_requests_guard`, lib.rs:1692) and make the raw
  pair non-pub.
- [ ] `configure`/`configure_with_options` silently no-op on second call
  (bool discarded by sole caller, s3-tests/server.rs:410); lazily-created
  `TRACE_SINK` means late configure leaves earlier lines in the env sink
  (lib.rs:3265-3283). Log rejected re-configure; document call-before-first-
  event.
- [ ] Dimension tables silently stop recording new label combinations at
  capacity 512 (lib.rs:556-571 + 8 siblings). Add a
  `dimension_overflow_total` counter.
- [ ] Three context conventions in one API (explicit `&TraceContext` vs
  thread-local with silent flight-record skip vs plain `event()` with no
  flight record: lib.rs:1954, 2406, 3071, 3240). Pick one; always write the
  flight record (crash-dump channel).
- [ ] `emit_metadata_command_checkpoint_record_error` is the only error
  emitter without a `*_TOTAL` counter (lib.rs:3071-3088).
- [ ] Idiom: all `emit_*` return an always-discarded `bool`; µs fields mix
  `u128`/`u64` with `saturating_u128_to_u64` sprinkled at use sites;
  `MetricsSnapshot` is `Copy` at ~976 bytes and passed by value; 4846-line
  single file with ~2000 lines of doubled emit boilerplate whose format
  strings have already drifted once (`emit_reclaim_queue_action`,
  lib.rs:2753-2778).

### Storage misc

- [ ] Metadata command payloads are inconsistent in carrying
  `BucketWriteReservationProof`: most object mutations embed it, but
  `AppendStreamSegment`, `DeleteCompletedMultipartUpload`,
  `AdvanceCompletedMultipartUploadSequence` carry none and `AbortStreamUpload`
  only an `Option` (`metadata_command.rs:692-1100`). Add a one-line doc
  comment per command stating which admission authority covers it.
- [ ] `ReserveObjectVersionCommand::matches_request` ignores `version_id`
  while the Delete twin compares it (`metadata_command.rs:679-681` vs
  844-851); safe only under the coordinator's version-allocation-under-lock
  invariant (guides/object-concurrency.md #7) which lives in another crate.
  Document the cross-crate assumption at the impl.
- [ ] Lock-poisoning policy split three ways (recover / panic / panic) across
  cluster.rs hot paths, test-hook installers, and storage_node_server.rs.
  Also server-http `mod.rs:4552` unwraps where server-core deliberately
  recovers (`coordinator.rs:77-79, 210-216`) — one panic while holding that
  write guard poisons the session and cascades into H1's 400 path. Pick one
  policy (e.g. a `lock_recovering` helper).
- [ ] `wall_time_millis` silently returns 0 pre-epoch (`clock.rs:57-62`) —
  flows into created_at/lease math as 1970 (compare A3); `with_time_override`
  (clock.rs:19) is exported without test gating. Gate behind `test-hooks`.
- [ ] Wildcard re-exports `pub use s3_types::lifecycle::*` and
  `pub use types::*` (lib.rs:86, 95) make the crate surface unauditable.
  Enumerate.
- [ ] Panics reachable from pub API worth restructuring or doc-noting:
  checkpoint-proof `.expect()` in the peering import path
  (cluster.rs:3293, 3311 — the code touched by "Close Raft checkpoint capture
  race"); `unwrap`/`unreachable!` in EC reconstruction read/backfill paths
  (cluster.rs:9493-10172); deliberate fail-fast `panic!` on invalid snapshots
  (control_plane.rs:2143, 3360, 3883) is fine but undocumented while
  `clippy::missing_panics_doc` is allowed crate-wide.

### Test-only footguns left pub

- [ ] server-http response builders that stamp the literal `"request-id"`
  into responses: `range_not_satisfiable`, `precondition_failed`, `forbidden`,
  `error` (`response.rs:381-383, 1210, 1468, 1939, 1960`). Mark `#[cfg(test)]`
  or delete in favor of `_with_ids` + `WireResponseIds::for_test()`.
- [ ] auth: `StreamingSigningContext`/`AuthContext` derive `PartialEq, Eq`
  over key material (request.rs:30, 57) — non-constant-time `==` on secrets;
  only tests use it. Drop or gate. Also `AuthContext` allows invalid
  anonymous/authenticated field combinations — an enum would remove the
  `Option`s (request.rs; consumers unwrap ad hoc).
- [ ] Minor idiom: no `#[must_use]` in ec pub API vs pervasive use in sibling
  crates; `clippy::must_use_candidate` globally allowed in server-core while
  hand-annotating; infallible `bucket_request` returns `Result`
  (mod.rs:243-253); `xml_escape`/`xml_unescape` asymmetric on `'`;
  dead `MAX_PRINCIPAL_LEN` (s3-types lib.rs:11); `ChecksumHasher` derives
  nothing while the CRC hashers derive Clone+Debug, and `finalize` receiver
  semantics differ within the crate; `RawChecksum::MAX_LEN` duplicated
  privately (types.rs:295 vs 337); `derive_signing_key` copies the secret
  into a transient heap String with no zeroization (sigv4.rs:137);
  streaming request types disagree on owned vs borrowed bucket/key
  (`request_types.rs:809-829`).

## Verified non-issues (for future review efficiency)

- Constant-time comparison used at all four auth signature/secret points plus
  the streaming chunk verifier; Debug redaction of secrets consistent.
- Credential-scope parsing shared by all three auth paths
  (`parse_credential_scope_ref`); disabled/unknown keys map identically.
- Bucket-policy operators/keys dispatch through single tables; Allow with
  unsupported conditions fails closed.
- CRC polynomials, combine implementations, and FULL_OBJECT part-combining
  seeding verified correct; round-trip enum pairs in checksum/s3-types
  consistent and tested.
- EC validation thorough when entered via `EcConfig::new`; both storage
  reconstruct call sites honor the sorted-indices precondition.
- Control-plane wire format disciplined (named fields, checksums, strict
  length/capacity checks, trailing-byte rejection); raft failover timers
  hardcoded and validated.
- Test hooks properly `#[cfg]`-gated; no consumer production code calls them.
- No `anyhow` on any pub surface; storage pub API is fully synchronous so
  async cancellation-safety concerns do not apply.
- Placement determinism within a build well tested; write/read paths share
  one key-derivation fn and one placer construction (order-insensitive).

## Review coverage gaps

Not reviewed; worth a follow-up pass:
- `server-http/src/http/xml.rs` serializers and `chunked.rs` aws-chunked
  decoder (grep-surveyed only).
- `control_plane.rs` / `control_plane_raft.rs` implementation bodies (pub
  surface and peering apply/publish paths only), `peering.rs`,
  `storage_rpc.rs` internals (pub(crate)).
- auth `condition_op.rs`/`condition_key.rs` per-operator bodies (dispatch
  verified only).
- AWS-behavior claims in H2 and H6 rest on documentation knowledge — pin with
  diff tests before changing behavior.
- Deterministic-log DInt64 arithmetic not verified bit-level against upstream
  CORE-MATH C.
