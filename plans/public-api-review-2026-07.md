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

In progress. Everything above the "Smaller items" section is done except S5
(deferred to the topology-resize plan): A1-A10, S1-S4, S6, K1-K4, H1-H8,
P1-P6, RR1-RR16, and V1-V6. A second verification pass on 2026-07-09
confirmed the V1-V6, P3-tail, and P4-P6 fixes against the code (crate checks
clean; server-core 1178 lib tests and the auth suite pass), and gave the new
control-plane auth surface its first pattern-class review. Results are in the
"Second re-review 2026-07-09" section below: one resolution-note claim was
wrong (the V1 checksum-type rejection IS reachable over HTTP on PutObject and
is unpinned against AWS — W1), and the control-plane auth review found three
substantive items (CA1-CA3) plus consistency/idiom follow-ups.

Remaining order of attack:
1. CA1-CA3 (control-plane auth: partial-role config leaves admin mutations
   open, optional verifier clock disables expiry, secrets in config parse
   errors) — highest value, these are new auth code.
2. W1/W2 (PutObject checksum-type: an unpinned behavior change — AWS-pin
   before it ossifies).
3. The remaining W (W3-W7) and CA (CA4-CA9) consistency/idiom items.
4. The "Smaller items" section below.

## Re-review 2026-07-05 — reopened and new items

### Reopened

- [x] **RR1. K1 core gap: `SystemMetadata::from_header_iter` was masked, not
  fixed — live bug on CreateMultipartUpload.** The K1 resolution fixed the
  HTTP layer but never made the core parser fail closed.
  `crates/server-core/src/system_metadata.rs:118-184` is still
  order-dependent and silent: a literal `x-amz-checksum-algorithm` header
  overwrites the algorithm derived from a concrete `x-amz-checksum-*` value
  header (mispair), and an unparseable value silently resets it to `None`.
  Server-http masks this on three of four call sites (PutObject filters the
  header at `mod.rs:421-426`; CopyObject REPLACE calls `strip_checksum_values`
  at `mod.rs:1473`; POST builds headers from an allowlist at
  `mod.rs:3482-3509`) — but the CreateMultipartUpload arm
  (`mod.rs:2637-2647`) feeds raw request headers through with no
  checksum-value validation at all. Verified consequence: an arbitrary,
  not-even-base64 `x-amz-checksum-sha256` value on CreateMultipartUpload is
  stored verbatim in the upload's system metadata
  (`coordinator/multipart.rs:397`), survives CompleteMultipartUpload when the
  upload has no checksum config (`multipart.rs:705-713` only overwrites when
  config is `Some`), is kept by `prepare_stored_system_metadata`
  (`object_state.rs:154-156`), and is served back on GET/HEAD. No test sends
  a concrete checksum value header on CreateMultipartUpload. Fix: make
  `from_header_iter` return an error on unparseable algorithm and on
  algorithm/value mispairing, independent of header order, and add the
  CreateMultipartUpload test. Fixed by making `SystemMetadata::from_header_iter`
  reject invalid literal checksum algorithms and literal/value algorithm
  mismatches, and by filtering checksum selection/value headers before metadata
  parsing on CreateMultipartUpload and CopyObject REPLACE. Added AWS-facing
  tests proving PutObject does not store literal `x-amz-checksum-algorithm`,
  CreateMultipartUpload accepts but ignores concrete checksum value headers
  without an algorithm, and a concrete checksum value header does not override a
  valid CreateMultipartUpload `x-amz-checksum-algorithm`.

- [x] **RR2. CRC64NVME unconfigured-Complete edge untested against AWS, and
  `requires_multipart_create_algorithm` is still dead code.** K2's fix wired
  the Complete-side predicate (`accepts_unconfigured_complete_multipart_header`)
  but `requires_multipart_create_algorithm` (`crates/checksum/src/types.rs:143-148`)
  still has zero call sites — neither used nor deleted. CRC64NVME sits in
  neither family predicate, so an unconfigured CompleteMultipartUpload with
  `x-amz-checksum-crc64nvme` falls into the reject arm
  (`multipart.rs:671-694`); oracle tests cover the legacy-ignored and
  new-rejected families but not crc64nvme, and since CRC64NVME is AWS's
  default full-object algorithm this edge plausibly diverges. Fix: AWS-pin
  the crc64nvme case, then wire or delete the dead predicate. Fixed by
  AWS-pinning that unconfigured CompleteMultipartUpload with
  `x-amz-checksum-crc64nvme` succeeds, computes and stores the real
  CRC64NVME `FULL_OBJECT` checksum, and ignores the supplied header value if
  it is a validly encoded mismatch. Local completion now uses that behavior
  for CRC64NVME only, while legacy algorithms remain accepted-and-ignored and
  newer non-CRC64 algorithms remain rejected. The dead
  `requires_multipart_create_algorithm` predicate was deleted. Added the
  matching PutObject oracle from the same AWS default-checksum rule: a
  PutObject with no checksum headers returns and stores a CRC64NVME
  `FULL_OBJECT` checksum, and an ignored literal `x-amz-checksum-algorithm`
  without a concrete checksum value follows the same default-storage path.

- [x] **RR3. H6 residual: "one 416 builder" not achieved; two dead pub
  builders left behind.** Behavior is correct and AWS-pinned, but there are
  still three live 416-producing paths, two byte-identical (conversion arms
  `response.rs:776, 780`; `range_not_satisfiable_with_ids` `response.rs:1416`,
  reachable only via the redundant explicit catch at `mod.rs:1722-1726` that
  a bare `?` would replicate). Dead code introduced by the fix:
  `S3Response::invalid_part_number` (`response.rs:1424`) — the purpose-built
  builder was never wired in — and `range_not_satisfiable`
  (`response.rs:1408`, stamps the literal `"request-id"`), both pub with zero
  callers. Consolidate to one builder, delete the dead pair. Fixed by routing
  the GetObject range branch through the normal `ServerError::InvalidRange`
  conversion and deleting the redundant public 416 builders; the existing
  `S3Response::error` conversion is now the single HTTP 416 formatter.

### New findings from fix verification

- [x] **RR4. Response XML shape dispatched by string equality on message
  literals duplicated across three crates.** `response.rs:36-46`
  (`is_host_id_invalid_request`) selects HostId-vs-Resource shape by exact
  message-string match against literals duplicated by value in
  `server-http/src/http/mod.rs:5349/5374/5380/5385`,
  `server-core/src/coordinator/multipart.rs:688` (prefix match), and
  `s3-types/src/lifecycle.rs:421`. Drift in any producer silently degrades
  the response to the Resource-shaped XML; only end-to-end s3-tests would
  catch it. Fix: shared constants or a typed shape flag on the error. Fixed
  by replacing the response formatter string match with typed
  `InvalidRequestHostId` errors. SDK checksum validation, unsupported checksum
  algorithm, unconfigured CompleteMultipartUpload checksum-type mismatch, and
  the lifecycle V1/NewerNoncurrentVersions parser case now construct or
  preserve the typed variant, and the HTTP formatter routes that variant
  directly to the HostId-shaped InvalidRequest XML.

- [x] **RR5. `RequestNotYetValid` response shape unpinned and inconsistent
  with its sibling.** The A4 variant falls through to the default formatter
  (`response.rs:914-921`: emits `<Resource>`, no `<HostId>`) — the opposite
  shape of `PresignedRequestExpired` (`response.rs:506-517`: HostId, no
  Resource). The oracle test (`presigned.rs:1881`) asserts only code and
  message. AWS-pin the shape and route through the AccessDenied formatter if
  confirmed. Fixed by tightening the AWS-facing presigned future-date test to
  assert `RequestId`, `HostId`, and no `Resource`; AWS confirmed that shape.
  Local `RequestNotYetValid` now routes through the same HostId-shaped
  `AccessDenied` formatter as `PresignedRequestExpired`.

- [x] **RR6. Dead `AuthError::InvalidToken`.**
  `crates/auth/src/error.rs:51` is no longer constructed anywhere in
  production code since the session-token removal (A2) — only mapped
  (`server-core/src/error.rs:404`) and matched. Either a producer is missing
  or the variant should be deleted. Fixed by deleting the dead auth variant
  and removing stale mappings/tests; static credential token inputs continue
  to return S3 `InvalidToken` through the live `UnexpectedSecurityToken` path.

- [x] **RR7. Presigned path accepts a signed `x-amz-security-token` header
  without rejection.** Header auth rejects any `x-amz-security-token` header,
  signed or not (`request.rs:329-333`); presigned auth checks only the query
  parameter (`request.rs:500-501`), so a token header listed in
  `X-Amz-SignedHeaders` passes the unsigned-header check and merely
  participates in the signature. Not exploitable with static credentials,
  but it is residual path drift with no test pinning it. Fixed by adding an
  AWS-facing presigned GET test for a signed `x-amz-security-token` header on
  static credentials: AWS returns `400 InvalidToken`, includes the standard
  malformed-token message and echoed token, includes `RequestId`/`HostId`, and
  omits `Resource`. Presigned auth now rejects a signed token header through
  the same `UnexpectedSecurityToken` path as token query parameters and
  header-auth token inputs.

- [x] **RR8. `route_map_validity_regressed` duplicated verbatim** at
  `storage/src/cluster.rs:1325` and `storage/src/storage_node_server.rs:11148`
  — a fresh instance of the P2 duplicate-entry-point pattern created by the
  S6 fix. Both copies also operate on `Option<u64>` projections rather than
  `RouteMapValidity` itself, keeping non-diagnostic Option plumbing alive.
  Consolidate into one helper taking the enum. Initially fixed by moving the
  regression predicate onto `RouteMapValidity::regresses_to`; V4 later removed
  that helper after RR14 made unbounded dynamic candidates an explicit illegal
  input and bounded shrinks remained permitted.

- [x] **RR9. Bucket write *drains* still have the pre-S1 shape.**
  `begin_durable_bucket_write_drain` takes `lease_deadline: Option<u64>`
  through trait and RPC (`traits.rs:163`, `storage_rpc.rs:1342`), and
  `clear_expired_durable_bucket_write_drain` only clears deadline-bearing
  rows (`traits.rs:190-192`) — a `None` drain row would be unreapable, the
  exact latent pattern S1 removed for reservations. All production callers
  pass `Some` today (`request_ops.rs:2958-2968`). Make the deadline required,
  matching S1. Fixed by making `BucketWriteDrainRecord.lease_deadline`,
  `begin_durable_bucket_write_drain`, and the Unix RPC drain-begin request use
  a required `u64`, serializing drain records with a required deadline, and
  making the SQLite `bucket_write_drains.lease_deadline` column `NOT NULL`.

- [x] **RR10. Single/batch peering validation scaffolding still duplicated.**
  S3's fix shares the authorization/proof helpers, but the surrounding
  ~50 lines of validation (acting-set membership, deterministic-primary,
  Peering-state, fence checks) are duplicated between the `CompletePgPeering`
  arm (`control_plane.rs:2068-2178`) and the batch arm (`:2179-2278`).
  Identical today; can drift again. Fixed by routing both command arms through
  a shared per-completion validator that owns the common acting-set,
  authorization, Active replay, deterministic-primary, Peering-state, fence,
  observation, and proof-floor checks. The batch arm keeps only its
  duplicate-PG check and its claimed proof/epoch matching as command-specific
  behavior.

- [x] **RR11. `Forever` encoded as a `u64::MAX` sentinel** in the route-map
  validity atomic (`cluster/local.rs:44-58`), making `Until(u64::MAX)`
  indistinguishable from `Forever`. Behaviorally harmless; add a doc note or
  debug_assert so the sentinel is pinned intentional. Fixed by making bounded
  route-map validity use a `RouteMapValidUntilMs` newtype that rejects
  `u64::MAX`, adding checked/saturating constructors on `RouteMapValidity`,
  rejecting the reserved sentinel during RPC/runtime-config decode, and keeping
  a debug assertion at the local atomic encoding boundary.

- [x] **RR12. Conditional date evaluation bypasses the shared clock
  abstraction** (`server-core/src/conditional.rs:14-19`). The current private
  `now_millis` helper uses `SystemTime` directly and maps pre-epoch clock
  errors to epoch 0 with `unwrap_or_default()`. The affected behavior is the
  RFC future-date guard for `If-Modified-Since` and copy-source
  `x-amz-copy-source-if-modified-since`; `If-Unmodified-Since` compares only
  the object `Last-Modified` timestamp against the header value. Low severity:
  a wrong-but-valid wall clock has no local error signal, but detectable clock
  errors should not be hidden. Use the shared clock abstraction or thread
  `now_millis` into the evaluator so the behavior is explicit and testable,
  and surface detectable clock errors instead of silently treating them as
  epoch 0. Fixed by deleting the private conditional clock helper and inlining
  `storage::clock::current_time_millis()` at both future-date guards, matching
  the rest of server-core's current clock policy. This intentionally does not
  add broader wall-clock sanity checks; an erroneous but valid wall clock will
  already affect TLS, SigV4, and other time-sensitive paths.

- [x] **RR13. Unreachable `AuthMode::PostSigV4` arm in
  `enforce_bucket_region`** (`mod.rs:3188-3192`): POST always authenticates
  with `ExactEndpointRegion`, so the arm can never fire, and its
  `InvalidCredentialScope` fallback is weaker than the in-auth errors. Add a
  comment (or make it unreachable explicitly) so a future POST-deferral
  doesn't silently take the weak path. Fixed by documenting the invariant in
  `enforce_bucket_region`: POST SigV4 form credentials must reject wrong
  regions during form authentication, before `AuthContext` construction. If a
  future refactor violates that invariant, the branch debug-asserts and
  returns `InternalError` instead of fabricating a weak or incomplete S3
  credential-scope response.

- [x] **RR14. Runtime-map validity wire format still permits `Forever`, and
  boundedness is same-epoch-checked only.** Control-plane RPC decode accepts
  an optional deadline (`control_plane.rs:5763, 5783` via
  `from_valid_until_ms(read_option_u64())`), and the epoch-increase refresh
  path has no boundedness check (`storage_node_server.rs:1782` covers same
  epoch only). Current control plane cannot issue `Forever` (all constructors
  bounded), so this is defense-in-depth: reject unbounded maps from
  authoritative sources at decode, or document why they are tolerated. Fixed by
  requiring bounded validity on runtime-map RPC decode and by rejecting
  unbounded dynamic refresh candidates in both frontend `StorageCluster` and
  storage-node runtime config install paths, including epoch-increase refreshes.

- [x] **RR15. Raft WAL surface (new since the original review) — minor
  pattern-class items.** Overall disciplined (private fields, typed errors,
  fully bounds-checked decode of persisted bytes; no hostile-input panics
  found). Items: `ControlPlaneRaftWalFile::new(path: impl Into<PathBuf>,
  cluster_name: impl Into<String>, node_id)` (`control_plane_raft.rs:4876`) —
  a `String` satisfies both leading params, transposition compiles;
  restore-path durability carried by name only (`from_restart_artifact` vs
  `_with_wal_file` share an inner fn where `wal: None` silently yields a
  non-durable log store, `:4310-4329`, `apply_record` skips persistence on
  `None` at `:4676`); dual replay entry points
  (`replay_log_store_artifact{,_from}`, `:4968/:4975`); confusable bare
  `Option<u64>` returns `durable_wal_base_offset()`/`durable_wal_clean_len()`
  (`:2470/:2475`). Fixed by making the WAL file constructor take a named
  `ControlPlaneRaftWalFileConfig`, renaming the non-durable log-store restore
  entry point to `from_restart_artifact_in_memory`, replacing the dual WAL
  replay methods with a single `ControlPlaneRaftWalReplayConfig` call shape
  that names the replay offset, and replacing the two bare public WAL-offset
  getters with one typed `ControlPlaneRaftWalOffsets` accessor.

- [x] **RR16. Cosmetic leftovers.** Test name
  `finalize_stream_put_rejects_mismatched_sse_s3_write_context`
  (`multipart_tests.rs:6832`) references the deleted SSE-S3 alias concept
  (stale: the removed alias layer is gone, but SSE-S3 remains the correct
  product/storage term for managed `AES256` object encryption, so the test
  name is still accurate);
  s3-tests copy helper form-urlencodes `versionId` (`helpers.rs:1849-1854`,
  `+` for space) while the parser strict-percent-decodes — inert today since
  version ids contain no spaces; small duplicated non-internal
  `error_response` closures remain in serve.rs (e.g. `:2979`). Fixed by
  switching the copy helper to strict SigV4 percent encoding and adding direct
  regression coverage, and by replacing the three streaming-handler closures
  with the shared `error_response` helper.

### Verification 2026-07-06 — RR fixes confirmed; residuals

All 16 RR items were adversarially verified against HEAD 52d7da15: fixes
landed as described, workspace `cargo check --all-targets` is clean, auth
crate tests pass (454), and the targeted control-plane peering tests pass.
RR1's reject paths in `from_header_iter` are defense-in-depth (every current
HTTP path filters or validates first), which is the intended layering. Small
residuals found by the verification, none release-blocking:

- [x] **V1. `from_header_iter` still handles two sibling fields the pre-RR1
  way.** An unparseable `x-amz-checksum-type` is silently dropped
  (`system_metadata.rs:176`, `ChecksumType::parse` → `None`, no error), and
  two different concrete `x-amz-checksum-*` value headers with no literal
  header resolve last-one-wins, order-dependently (:178-182). Both are
  masked by HTTP-layer validation/filtering today — exactly the masking
  shape RR1 removed for the algorithm field. Extend the fail-closed
  treatment to `checksum_type` and to conflicting value headers. Fixed by
  making `SystemMetadata::from_header_iter` reject invalid checksum type values
  and multiple concrete checksum value headers for different algorithms, with
  direct parser regressions.

- [x] **V2. MPU default-checksum asymmetry unpinned.** Unconfigured
  CompleteMultipartUpload with no checksum header used to store no checksum
  at all, while PutObject with no checksum headers stores default CRC64NVME
  (RR2's own oracle). Fixed by adding the AWS-facing
  `test_complete_multipart_without_checksum_headers_defaults_crc64nvme`
  oracle, which confirms AWS returns and stores CRC64NVME/FULL_OBJECT for
  this case, and by aligning local CompleteMultipartUpload to store the same
  default checksum while preserving AWS's ignored-legacy-header behavior.

- [x] **V3. Third message/shape variant for unsupported checksum algorithm.**
  The new core rejection emits plain `InvalidRequest` with
  `"invalid checksum algorithm: {value}"` (`system_metadata.rs:171`) while
  the HTTP layer's equivalent is HostId-shaped `InvalidRequestHostId` with
  the AWS message (`mod.rs:5363`). Unreachable via HTTP today (callers
  filter first); align the message/shape so a future unfiltered caller
  matches AWS. Fixed by moving the AWS-shaped unsupported checksum algorithm
  message into `ServerError::unsupported_checksum_algorithm()` and using that
  shared constructor from both `SystemMetadata::from_header_iter` and the HTTP
  checksum-algorithm parser, with a direct parser regression for the
  `InvalidRequestHostId` shape.

- [x] **V4. `RouteMapValidity::regresses_to` is now dead, and the rejection
  error name does double duty.** RR14's unconditional unbounded-candidate
  rejection subsumes the `regresses_to` disjunct at both install paths
  (`cluster.rs:1138-1143`, `storage_node_server.rs:1789-1794`) — the helper
  can never return true from a live call site. Also, an unbounded candidate
  at a higher epoch is reported as `ValidityRegression`/
  `RuntimeRefreshValidityRegression { candidate: None }`, which is not a
  regression relative to current. Remove the dead disjunct/helper (or keep
  and document as belt-and-braces) and name the unbounded-candidate
  rejection distinctly. Fixed by deleting `RouteMapValidity::regresses_to` and
  replacing the old regression errors with explicit unbounded dynamic
  candidate errors:
  `StorageClusterRuntimeMapRefreshError::UnboundedRouteMapValidity` and
  `StorageNodeServerError::RuntimeRefreshUnboundedRouteMapValidity`.

- [x] **V5. Security-token check ordering differs by auth path**
  (pre-existing, now pinned). AWS-facing tests now cover
  bogus-signature-plus-token requests for header SigV4, presigned SigV4, and
  POST Object SigV4; AWS returns `SignatureDoesNotMatch` on all three paths.
  Header auth already matched. Presigned and POST now validate static security
  tokens only after signature verification, so the paths share the AWS-pinned
  precedence while still returning `InvalidToken` for valid-signature
  unexpected-token requests.

- [x] **V6. Cosmetic.** `until_ms_saturating` clamps `u64::MAX` silently
  (`types.rs:2612-2618`) where RR11 debug-asserts at the atomic boundary —
  add the same debug_assert for consistency; checksum header-name filter
  literals duplicated between `parse_put_object_request_metadata`
  (`mod.rs:426`) and `parse_request_metadata_without_checksum_headers`
  (:436-440); `LifecycleConfigError::InvalidRequestHostId` leaks an
  HTTP-response-shape concern into s3-types (clean typed mechanism,
  awkward name). Fixed by adding the route-map sentinel debug assertion,
  introducing `is_checksum_algorithm_header_name` for shared metadata filters,
  and renaming the s3-types lifecycle error to `LifecycleV2Required` while
  keeping the server-layer mapping to `InvalidRequestHostId`. Historical note,
  not actionable: server-http test-target compilation was broken in the
  4c01bac9..4d05ea01 commit window (fixed by a drive-by in 496d5f6e; already
  fine at HEAD).

## Second re-review 2026-07-09 — verification results and new items

A verification pass at HEAD aab790f0 adversarially confirmed the V1-V6,
P3-tail, and P4-P6 fixes: all landed as real fixes (named params structs
threaded through every layer, typed dimension enums exhaustive with no
catch-all arms — `finalize_started` finally counts, typed error enums with
structured payloads, `LifecycleRuleFilter`/`StorageNodeProcessConfig`
privatized behind validating constructors, `regresses_to` deleted with
properly-named replacement errors). `parse_bucket_policy` was correctly
resolved as invalid with AWS pinning (the cap applies to the normalized
policy, matching AWS). server-core 1178 lib tests, the auth suite, and crate
checks all pass. The pass also gave the new control-plane auth surface
(commits 79f04c5c..510eb737) its first pattern-class review — fundamentals
are strong (single constant-time `ring::hmac::verify` path everywhere,
consistent secret redaction with a dedicated test, fully bounds-checked
envelope decode, the multi-node Raft auth gate unbypassable via config), with
the findings below.

### Residuals from the verified fixes (W series)

- [x] **W1. V1's checksum-type rejection is HTTP-reachable on PutObject and
  unpinned against AWS** — the resolution note's "masked by HTTP-layer
  validation" claim is false for this field.
  `parse_put_object_request_metadata` filters only `x-amz-checksum-algorithm`
  (`server-http/src/http/mod.rs:443-448`), not `x-amz-checksum-type`, and no
  PUT-path pre-validation of that header exists (only CreateMultipartUpload
  validates it, mod.rs:2730). So PutObject + `x-amz-checksum-type: garbage`
  changed from 200-with-header-ignored to 400 `InvalidRequest` — an
  observable behavior change with no AWS oracle (`checksums.rs` has no
  PutObject+checksum-type coverage at all). Adjacent pre-existing gap: a
  *valid* `x-amz-checksum-type: COMPOSITE` on plain PutObject flows into
  stored metadata unvalidated against the algorithm. AWS-pin PutObject with
  invalid and valid-but-inapplicable checksum-type headers, then align.

  Fixed 2026-07-09: AWS-facing `s3-tests::checksums` now pins that
  `PutObject` ignores `x-amz-checksum-type` entirely: invalid values are
  accepted, `COMPOSITE` is accepted for single-part `PutObject`, and stored
  metadata/response headers report `FULL_OBJECT` for CRC32, CRC64NVME, and
  SHA256 checksum value headers. The local `PutObject` metadata parser now
  filters `x-amz-checksum-type` before `SystemMetadata` parsing, so invalid
  values do not reject and explicit `COMPOSITE` cannot be stored for
  single-part PUT.

- [x] **W2. V3's message/shape divergence reintroduced for checksum-type.**
  Core emits plain `InvalidRequest` `"invalid checksum type: {value}"`
  (`system_metadata.rs:180`) while the HTTP MPU path emits `InvalidArgument`
  `"unsupported checksum type: {v}"` (`mod.rs:2734`) — two shapes/messages
  for the same field, and unlike V3's case the core one IS reachable over
  HTTP (W1). Same fix shape as d1f22148: shared constructor + AWS-pinned
  message.

  Fixed with W1: the core `SystemMetadata` checksum-type parse error is no
  longer reachable through `PutObject`; CopyObject REPLACE and
  CreateMultipartUpload already strip checksum-type from generic metadata
  parsing and handle checksum-type at their HTTP boundaries. The remaining
  checksum-type HTTP validation surface is MPU-specific and keeps the
  AWS-pinned MPU error shape.

- [x] **W3. Same-algorithm duplicate value headers are now first-wins at the
  parser** (`system_metadata.rs:186-198` — pre-fix was last-wins, so the
  order-dependence flipped rather than disappeared); masked over HTTP by
  `extract_encoded_checksum_header`'s duplicate rejection. Also the
  conflicting-value-headers unit test covers only one header order
  (`system_metadata.rs:715`) — the logic is symmetric by construction, but
  add the mirror order. Make the parser reject same-algo duplicates too.

  Fixed 2026-07-09: `SystemMetadata::from_header_iter` now rejects duplicate
  same-algorithm checksum value headers with `DuplicateChecksumHeader`, matching
  the HTTP checksum-claim duplicate policy instead of keeping the first value.
  Unit coverage now includes duplicate same-header values in both orders and the
  mirror order for conflicting checksum algorithms.

- [x] **W4. Duplicated credential-expiry check.** c3be106b inlined the expiry
  check in `auth/src/sigv4.rs:176-178` instead of calling
  `validate_static_record_expiry` (`request.rs:633-641`) — two copies of the
  same policy that can drift. Consolidate.

  Fixed 2026-07-09: header SigV4 verification now calls the shared
  `validate_static_record_expiry` helper used by presigned and POST SigV4
  authentication. Existing header-auth tests continue to pin expired-token
  behavior, including the case where expiry is reported before signature
  mismatch.

- [x] **W5. `LifecycleRuleFilter::explicit_with_predicates` validates prefix
  and tags but not size ordering** (`lifecycle.rs:101-118`) —
  `(None, vec![], Some(10), Some(5))` builds a min>=max filter the parser
  rejects; and its two adjacent `Option<u64>` size params are themselves a
  transposition hazard that produces exactly that state. Validate size
  ordering in the constructor and take a small size-bounds struct (or one
  `SizeRange` newtype) instead of the bare pair.

  Fixed 2026-07-09: `LifecycleRuleFilter::explicit_with_predicates` now takes
  a `LifecycleObjectSizeRange` instead of adjacent `Option<u64>` parameters.
  The range constructor shares the parsed XML validation rule and rejects equal
  or inverted bounds with the AWS-pinned lifecycle error message, so the public
  constructor can no longer build a filter state the XML parser would reject.

- [x] **W6. `bucket_lock_wait_exceeded_total` is inert in production builds.**
  Original finding: the P4 metrics sweep exported
  `bucket_lock_wait_exceeded_total`, but its only increment path was tied to
  the legacy `SharedStorageNode::lock_bucket` stripe lock, so it always
  reported 0 in production.

  Fixed 2026-07-09: the obsolete bucket stripe lock was removed completely
  instead of preserved as a test-only tripwire. This removed
  `SharedStorageNode::lock_bucket`, `test_lock_bucket`, the associated
  bucket-lock test hook, stale tests that only exercised that lock, and the
  inert `bucket_lock_wait_exceeded_total` metric/export. Remaining lock
  contention tests use production-shaped PG/metadata-command boundaries.

- [x] **W7. Smaller leftovers.** `PgTopology`'s typed error isn't fully
  honored: `node.rs:509, 582` still `.expect()` on empty `pg_ids` inside
  `Result`-returning production constructors (`.unwrap()` is gone but the panic
  isn't). `emit_metadata_command_recovery_admission` (observability
  lib.rs:3038-3056) still matches `admission: &str` with `_ => {}` fallthrough
  (the P5 class, out of that sweep's scope; callers pass a closed literal set
  today). `UnixStorageNodeRpcAdmission::new_with_wait_timeout(usize, Duration,
  Duration)` (`node_client/unix_admission.rs:144-148`) keeps an adjacent
  Duration pair one layer below the named settings struct.

  Fixed 2026-07-09: `SharedStorageNode` constructors now map
  `PgTopologyError` into `StoreError::InvalidPgTopology` instead of panicking,
  with regressions for both regular and topology-only opens.
  `MetadataCommandRecoveryAdmissionSummary` now carries a closed
  `MetadataCommandRecoveryAdmissionKind` enum, so unknown labels cannot be
  silently dropped. `UnixStorageNodeRpcAdmission` now takes
  `UnixStorageNodeRpcAdmissionSettings`; the higher-level local Unix client
  admission settings convert into that lower-level settings object instead of
  passing adjacent raw timeouts.

### Control-plane auth — first pattern-class review (CA series)

The control-plane auth surface (envelopes, scoped credentials, Raft peer auth,
rotation; commits 79f04c5c..510eb737) landed since the original review and had
never been checked. Fundamentals are strong (see the intro above). Findings,
ranked:

- [x] **CA1. Partial-role Unix control-plane auth leaves admin mutations
  unauthenticated with no config coupling.** Dispatch gates each path on that
  role's credential map being non-empty (`requires_*` is `!map.is_empty()`,
  `control_plane.rs:6402-6415`; enforced at :7410-7426), and config validation
  (`argmin-s3/src/config.rs:780-841`, verifier build `main.rs:3330-3384`)
  accepts any subset of STORAGE/FRONTEND/ADMIN. An operator who configures only
  frontend credentials gets authenticated runtime-map *reads* while
  `SetPgActingSet`, `TransferRaftLeadership`, `TriggerRaftSnapshotAndPurge`,
  `TriggerRaftElection` stay fully unauthenticated on the same socket — the
  strongest operations left open, silently. The P1 "None weakens semantics"
  class, spelled as empty-map. Known/deferred to cutover per the plan (per-path
  `required` flags are exposed), but nothing stops the foot-gun config. Fix:
  config-parse error when some-but-not-all credential sets are configured, or
  require admin credentials whenever any Unix control-plane auth is on, with an
  explicit opt-out. Resolution: the serving control-plane configuration and
  verifier builder now reject any Unix control-plane auth configuration that
  omits `ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS`. Client-only storage and
  frontend roles can still load scoped credentials for signing their own
  requests, but a process serving the control-plane socket cannot enable
  storage/frontend auth while leaving admin RPCs open. Added config and verifier
  regressions for storage/frontend auth without admin credentials.

- [x] **CA2. `ControlPlaneAuthVerificationInput.now_ms: Option<u64>` — `None`
  silently disables expiry checking, and timestamp-less envelopes skip it even
  under `Some`.** `verify_envelope_inner` runs issued/expires checks only
  `if let Some(now_ms)` and `is_some_and` per field
  (`control_plane_auth.rs:459-472`). Because the core verifier can't require
  timestamps, every path grew its own pre-validator
  (`validate_control_plane_unix_read_auth_freshness` control_plane.rs:6905;
  `validate_storage_node_heartbeat_auth_freshness` :6843;
  `validate_peer_auth_replay_window` control_plane_raft.rs:806) — the P1+P2
  combination, freshly minted in auth code. Fix: replace `now_ms: Option<u64>`
  with an enum (`ReplayPolicy::TimestampWindow { now_ms, max_window_ms }` vs
  `FencedByPayloadSemantics`) that rejects missing-or-stale timestamps centrally
  and collapse the three per-path validators into it. Resolution: the verifier
  now takes `ControlPlaneAuthReplayPolicy` instead of optional `now_ms`.
  Timestamp-window paths centrally require `issued_at_ms` and `expires_at_ms`,
  reject empty/overlong/future/expired windows as `ReplayFreshnessFailure`, and
  make response skew an explicit policy parameter. Raft peer RPCs that are
  fenced by term/log/payload semantics now choose `FencedByPayloadSemantics`
  explicitly, while transfer-leader uses a bounded timestamp policy. The Unix
  read/admin/heartbeat and Raft peer per-path freshness validators were removed.

- [x] **CA3. Credential env parse errors echo the raw entry, which contains the
  secret.** All four parsers format the offending entry into the error
  (`argmin-s3/src/config.rs:1193, 1278, 1359, 1439`), so a malformed
  `RAFT_AUTH_CREDENTIALS`/storage/frontend/admin entry (e.g. a missing `=`)
  puts the secret verbatim into a startup error to stderr/logs/supervisors —
  undercutting the otherwise consistent `SecretConfigValue` redaction. Fix:
  report entry index/node-id only, never the raw entry text. Resolution: auth
  credential parser errors now identify malformed entries by env var and entry
  number until a safe principal id is parsed, and credential-version failures no
  longer echo the raw version field. Added regressions for Raft, storage,
  frontend, and admin credential parsing covering missing `=` and missing
  version fields without leaking the raw entry or secret.

- [x] **CA4. Expired envelope misclassified as `StaleCredential`.**
  `control_plane_auth.rs:466-471` returns `StaleCredential` for
  `expires_at_ms <= now_ms`, which everywhere else means "a newer version
  exists". Observable on the transfer-leader path (whose pre-check validates
  only window width): an expired frame lands in `rejected_by_reason
  {StaleCredential}` instead of `ReplayFreshnessFailure` in the redacted
  diagnostics. Fix: return `ReplayFreshnessFailure` for envelope expiry.
  Resolution: expiry now runs through the centralized timestamp replay policy,
  so expired envelopes are rejected as `ReplayFreshnessFailure`; stale credential
  remains reserved for credential-version supersession.

- [x] **CA5. Multi-peer ⇒ auth invariant lives only in argmin-s3 config, not
  the transport type.** `ControlPlaneRaftPeerTransportPolicy.auth_policy:
  Option<Arc<...>>` defaults to `None` and both directions fall through to
  plaintext when `None` (`control_plane_raft.rs:847, 1469-1573`). The fa8a5fa4
  config gate is real and unbypassable via argmin-s3 (verified), but any other
  embedder of the storage crate can build a multi-peer auth-less transport with
  no complaint. Fix: enforce in the constructor (>1 peer without an auth policy
  → `Err`; provide a named single-node/test constructor). Resolution: reviewed
  against the current Unix/TCP auth policy and treated as invalid for the Unix
  transport. Multi-peer Unix sockets may be legitimate for local/test and
  single-host layouts where filesystem permissions and private socket placement
  are the trust boundary, including multi-disk-per-host deployments. The hard
  mandatory-auth invariant belongs on future TCP/non-local control-plane
  transports and their config-file startup path, not on the reusable Unix peer
  transport type. Keep the TCP constructor/config gate as the follow-up
  enforcement point.

- [x] **CA6. Per-path verifier/signer/selection copies that can drift (P2).**
  Three ~150-line near-identical verify methods
  (`verify_admin_control_plane_command_payload` control_plane.rs:6426,
  `verify_frontend_runtime_map_read_payload` :6574,
  `verify_storage_node_heartbeat_payload` :6710); two response signers
  identical but for the operation constant (:6987, :7030); two client response
  verifiers with the same skeleton (:5628, :5939); four copies of "select
  latest credential (version, then id)" (`main.rs:1925-1937, 3523, 3542,
  3561`). Consistent today, nothing keeps them so. Fix: one
  `latest_by_version_then_id` helper and a shared role-parameterized verify
  core. Resolution: the low-risk drift points were removed now. Response
  signing now uses one operation-parameterized helper, and Raft/storage/
  frontend/admin local credential selection uses one
  `latest_auth_credential_by_version_then_id` helper. The larger
  role-parameterized verifier-core extraction is intentionally deferred until
  CA7 because heartbeat payload binding will change the storage-node verifier
  shape; doing that extraction first would churn the same code twice.

- [x] **CA7. Heartbeat envelopes don't bind the outer RPC kind; every other
  Unix path does.** Frontend/admin/all responses wrap via
  `write_authenticated_control_plane_rpc_payload(kind, ...)` and check it; the
  heartbeat path signs the bare payload (`control_plane.rs:7279-7297`) and reads
  from `envelope.payload()` (:6731). Safe only because
  `StorageRuntimeMapRefresh` ↔ `RefreshNodeHeartbeat` is 1:1; a second kind
  mapping to that operation would open cross-kind replay. Fix: add the kind
  prefix to heartbeat envelopes. Resolution: authenticated storage-node
  heartbeat requests now sign the same kind-prefixed payload shape as the other
  Unix control-plane auth paths, the verifier unwraps `RefreshNodeHeartbeat`
  before parsing the heartbeat, and regression coverage rejects a signed
  heartbeat envelope with a mismatched embedded RPC kind before lease mutation.

- [x] **CA8. Dead wire-format surface / always-empty replay fields.**
  `ControlPlaneAuthOperation::StorageHeartbeat` (wire tag 6) and
  `ControlPlaneAuthService::{RaftPeerTransport, StorageNodeControl}` have no
  production sign/verify site; every signer passes `sequence: None,
  nonce: Vec::new()` and no verifier checks nonce uniqueness (replay is
  timestamp-window + idempotency, per the plan). Fix: delete the dead variants
  (pre-release) or wire them; document nonce/sequence as reserved. Resolution:
  removed the unused service/operation variants from the public auth vocabulary
  so their old tags fail closed as unknown values, documented sequence/nonce
  fields as reserved and not replay-tracked by current verifiers, and added the
  production nonce/sequence replay-cache decision to the Phase 12.4 rollout
  scope.

- [x] **CA9. Positional same-typed params (P4, in new code).**
  `ControlPlaneAuthEnvelope::new(header, payload, authenticator)` — two
  positional `Vec<u8>` (`control_plane_auth.rs:700-712`);
  `ControlPlaneFrontendAuthCredential::new`/`...AdminAuthCredential::new` —
  adjacent `impl Into<String>` pairs (`control_plane.rs:6054, 6117`). Fix:
  input structs (matching `ControlPlaneScopedCredentialInput`). Also worth a
  comment on the Raft inbound verifier: `expected_operation`/source are derived
  from the attacker-supplied envelope, so those equality checks are tautological
  there — real safety is the credential lookup + MAC + binding cross-check
  (traced: reflection/response-as-request/cross-target replays all fail closed).
  Resolution: converted auth envelope and Unix credential constructors to named
  input structs, including storage-node credentials for consistency, and added a
  Raft peer listener comment documenting that predecoded envelope fields select
  metrics/expected identity only; credential lookup, MAC verification, and
  authenticated payload binding remain the trust boundary.

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
  `authenticate_header_unsigned_amz_header` for the presigned path.
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
  concrete deadline, and DeleteBucket reservation wait releases exact expired
  reservation rows before deciding the bucket is blocked. Proof matching
  (`matches_record`, metadata_command.rs:971-984) deliberately excludes the
  mutable `lease_deadline` — freshness is checked separately and the heartbeat
  CAS is the only deadline-inclusive match — see the documented contract under
  P2. Added a local-cluster regression covering an unreleased expired durable
  reservation being reaped during DeleteBucket. Re-review 2026-07-05: verified;
  deadlines are bounded (now + 15s lease, no `u64::MAX`). The *drain* API kept
  the old optional-deadline shape — tracked as RR9.

- [x] **S2. Invalid: new `buckets` column added without a migration.** This
  finding assumed pre-alpha stores are upgraded in place. They are not:
  `plans/storage-upgrade-versioning-plan.md` documents the explicit no-upgrade
  policy, and `guides/threat_model.md` states older database schemas are not
  supported until a future stability point. Do not add idempotent `ALTER`s for
  this. Existing speculative baseline migrations should be audited and removed
  under Phase 0 of the storage upgrade/versioning plan.

- [x] **S3. `CompleteReadyPgPeerings` skips the node-service authorization
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
  behavior. Resolution: `ReadyPgPeeringCompletion` now carries
  `node_incarnation`, `CompleteReadyPgPeerings` authorizes each completion with
  the same node-service lease/incarnation boundary as `CompletePgPeering`, and
  exact Active replays are treated as no-op completions instead of
  `PgNotPeering`. The control-plane command encoding baseline was bumped
  because pre-alpha stores are not upgrade-supported yet. Re-review
  2026-07-05: verified — both paths call the same
  `authorize_node_service_for_snapshot`; the surrounding validation
  scaffolding is still duplicated between the single and batch arms (RR10).

- [x] **S4. Tautological `matches_request` argument neutralizes the
  generation check.** `cluster.rs:553-555` passes
  `commit.object.generation_id` as the generation argument to
  `commit.matches_request(...)`, making the comparison in
  `metadata_command.rs:697-709` vacuously true. Sibling caller
  `node_client.rs:659` passes a real request generation. Fix: split into
  `matches_session(...)` and `matches_request(..., GenerationId)` so
  "don't-care" must be explicit. Resolution: direct PUT commands now have an
  explicit `matches_stream_session` helper for pending stream terminalization;
  request/retry paths continue to use generation-aware `matches_request`.

- [ ] **S5. MPU-cleanup resume cursor is a positional index, not a PG id.**
  `delete_completed_multipart_uploads_for_bucket`
  (`cluster/request_ops.rs:6178-6196`) persists `next_pg_index`
  into the call-time-sorted `metadata_pg_ids()`. Since the review a bounds
  check was added (errors if cursor > metadata PG count), but the resize
  re-targeting hazard is unchanged. The current implementation
  sorts the PG list and has sparse-PG regression coverage, so this is not a
  current bug while the configured PG set is fixed for the cluster lifetime.
  It is a real topology-resize hazard: if the metadata PG set changes between
  crash and resume, the index silently re-targets different PGs, skipping
  cleanup on some. Track this under
  `plans/storage-topology-resize-plan.md` H5. A resize-safe fix should persist
  a semantic cursor such as last-completed `PgId` plus topology generation, or
  make the cleanup phase generation-scoped and restartable from zero when the
  PG set changes.

- [x] **S6. Route-map validity is a storage routing freshness contract, not a
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

  **Status:** fixed. Route-map validity is now modeled as
  `RouteMapValidity::{Forever, Until(RouteMapValidUntilMs)}` in runtime-map
  snapshots, storage-node process config, local cluster maps, and refresh-loop
  status.
  Current-format storage-node runtime config serializes this explicitly as
  `route_map_validity forever` or `route_map_validity until <ms>` and no
  longer accepts the legacy `valid_until none` spelling. Test fixtures now
  construct `Forever`/`Until` directly instead of passing `None` through
  helper APIs. Control-plane-issued runtime maps are always bounded: active
  maps use the minimum primary lease deadline, non-serving maps without a
  lease-bearing route get a bounded fallback freshness window rather than an
  immediate expiry, and reconstructed historical views preserve the source
  snapshot validity instead of inventing an unbounded map. Refresh/install
  paths reject same-epoch `Until -> Forever` regressions, while same-epoch
  refresh propagation can still bound previously pinned static/local
  generations when an authoritative bounded map is installed. RR11 then made
  bounded deadlines use `RouteMapValidUntilMs`, which rejects `u64::MAX` so
  the local atomic `Forever` sentinel remains internal. The `valid_until_ms()`
  accessors remain only as deadline projections for diagnostics and error
  payloads. Re-review 2026-07-05: verified; RR14 removed wire-format `Forever`
  tolerance for runtime-map RPC snapshots and dynamic refresh installs. RR8
  removed the duplicated route-map validity regression helper.

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
  `crates/s3-tests/tests/checksums.rs`. Re-review 2026-07-05: the HTTP-layer
  fix is verified, but the core `from_header_iter` fail-closed directive was
  not implemented and the CreateMultipartUpload call site is unprotected —
  reopened as RR1.

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
  `crates/s3-tests/tests/checksums.rs`. Re-review 2026-07-05: RR2 verified
  that CRC64NVME is a separate AWS behavior: an unconfigured
  CompleteMultipartUpload with `x-amz-checksum-crc64nvme` computes and stores
  the real full-object checksum, even if the supplied header value is a valid
  mismatch. The same AWS default-checksum rule is now pinned for PutObject:
  no checksum headers returns and stores CRC64NVME `FULL_OBJECT`, and an
  ignored literal `x-amz-checksum-algorithm` without a concrete checksum value
  takes the same stored-default path.

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
  CompleteMultipartUpload `InvalidPart` semantics unchanged. Re-review
  2026-07-05: behavior verified, but the single-builder consolidation did not
  happen and two dead pub builders were left behind — residual tracked as RR3.

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
- [x] auth: `pub verify_request` (sigv4.rs:156-170, re-exported lib.rs:75) is
  an attractive-but-incomplete verification door (no skew/expiry/token/scope)
  with zero external callers. Resolution: delete the public wrapper and
  re-export; the internal verifier remains `verify_request_record` behind
  `authenticate_request`. Existing SigV4 request tests now exercise
  `authenticate_request` rather than the partial verifier.
- [x] auth: private, dead `combine_decisions` (deny-wins collapse,
  `bucket_policy/evaluator.rs:137-151`) while server-core hand-rolls the
  three-way match at `authz/policy.rs:499-519` and `authz/modern.rs:889-931`.
  Resolution: delete the unused evaluator seam instead of exporting it.
  Server-core's authorization paths also apply S3 ownership, public-policy,
  fallback, and root-principal rules, so the dead helper was not a drop-in
  production abstraction. Future IAM work can refer back to
  `plans/completed/bucket-policy-evaluator-structure-plan.md` for the old
  design sketch when a real second policy source exists.

### P3. Validating constructors bypassed by public fields

No live bug in any of these (all current callers use the constructors), but
each is one refactor away from a panic:

- [x] `EcConfig` pub fields (`crates/ec/src/codec.rs:13-18`): `k=0` panics in
  encode/verify/reconstruct (codec.rs:332, 416, 568); `k+m > 32` overruns the
  fixed 32×32 stack matrices in `reconstruct_shards`
  (`crates/ec/src/reconstruct.rs:18-26`). `ErasureCodec::new` returns `Result`
  but had no failure path (codec.rs:254-284). Resolution: make the fields
  private, add read-only accessors, and defensively revalidate in
  `ErasureCodec::new`.
- [x] `PlacementConfig.total_shards` pub field (`crates/placement/src/config.rs:7`):
  bypasses `new()` validation; `Placer::place` then writes past `MAX_SHARDS`
  stack arrays (placer.rs:133, 206) — out-of-bounds panic in the hot path.
  Resolution: make the field private, change `new(total_shards)` to take
  `usize`, add a read-only accessor, defensively revalidate in `Placer::new`,
  and remove the lossy `as u8` from the storage placement caller.
- [x] `StorageNodeProcessConfig`: still 9 pub fields (now including
  `route_map_validity: RouteMapValidity` after S6) whose consistency is
  enforced only by the separate `validate_storage_node_process_configs`
  (`storage_node_server.rs:325-335, 1335`). Resolution: remove the public
  field bypass by making fields crate-visible to storage internals/tests only,
  add `StorageNodeProcessConfig::new(StorageNodeProcessConfigParts)` with route
  table validation, move the external `argmin-s3` construction path through the
  validating constructor, and expose read-only accessors for external callers.
- [x] `SseCustomerRequest::with_algorithm` accepts any string, breaking the
  AES256 pin that `new()` establishes (`sse.rs:54-57`); validation lives only
  in server-http (callers `mod.rs:5005, 5117`). Resolution: remove the setter
  and make the algorithm implicit in the core type; HTTP parsing still rejects
  non-`AES256` inputs before constructing `SseCustomerRequest`, while
  `algorithm()` returns the canonical constant.
- [x] `WriteEncryptionRequest::from_request_parts` has
  `unreachable!` on conflicting SSE-C+SSE-S3 inputs
  (`coordinator/request_types.rs:317-326`) — invariant enforced in a
  different crate, 5 call sites; with `abort_on_500` a future mistake is a
  process abort. Resolution: make the constructor fallible, return
  `InvalidArgument` with the existing SSE-C/SSE-S3 conflict message, update the
  HTTP callers to propagate it, and add a core regression for the direct
  conflicting-input path.
- [x] `LifecycleRuleFilter` pub fields permit states the parser never
  produces (legacy rule with tags) which render as V2 `<Filter>`
  (`s3-types/src/lifecycle.rs:37-43`). Resolution: make the filter fields
  private, add constructors for legacy prefix and explicit filter forms, expose
  read-only accessors, validate tag filters through the constructor, and move
  external construction sites to the canonical constructors so callers cannot
  create a legacy-style filter with tag or size predicates.

### P4. Positional same-typed parameters

- [x] Claim acquire fns take 3 consecutive `u64` timestamps + 2 `&str` tokens
  (`cluster.rs:8794-8802 acquire_placed_segment_shard_repair_claim`,
  `cluster.rs:9155-9162 acquire_next_placed_segment_shard_backfill_claim`);
  consumers pass `now_ms` in two of the three slots
  (`server-core/src/coordinator/runtime.rs`). The params structs already
  exist (`types.rs:1593 PlacedSegmentShardRepairClaimAcquire`,
  `types.rs:1638`) — use them in the signatures. Fixed by adding
  cluster-facade params structs with named `claim_id`, `owner_token`,
  `claimed_at`, required `lease_deadline`, and `now` fields, making
  `StorageCluster` stamp `cluster_epoch`, and changing the domain acquire
  structs to require `lease_deadline: u64` while leaving optional deadlines
  only at malformed RPC boundaries.
- [x] `acquire_durable_bucket_write_reservation`: still positional across all
  layers (`traits.rs:94-104`, `node_client/local.rs:727, 1763`,
  `pg_store/metadata.rs:5377-5387`); the S1 fix made `lease_deadline` a
  required `u64` but kept the positional list, so `created_at`/`lease_deadline`
  are now two adjacent bare `u64`s — the transposition hazard is marginally
  worse. The same trait's heartbeat API already uses a params struct.
  Fixed by introducing `DurableBucketWriteReservationAcquire<'_>` and using it
  through the store, node-client, local, Unix RPC, and storage-node dispatch
  layers while keeping `PgId` as separate routing context.
- [x] `PgMetadataProof::new(u64, u64, u64)` (`control_plane.rs:3106`) —
  index/hash/digest transposition compiles silently and poisons peering-proof
  comparison. Fixed by removing the positional constructor and converting
  callers to named `PgMetadataProof { applied_log_index, applied_log_hash,
  state_digest }` construction.
- [x] `TraceContext::from_ids(String, String)` (`observability/src/lib.rs:39`)
  — fixed by replacing the positional constructor with
  `TraceContextIds { trace_id, request_id }`, so wire-boundary callers must name
  both IDs.
- [x] `put_bucket_acl_and_load_info(..., public_read: bool, public_write:
  bool)` (`request_ops.rs:6668-6674`) — fixed by introducing
  `BucketAclSummary { public_read, public_write }` and threading it through
  authorized ACL results, metadata command construction, local/Unix node
  clients, RPC validation, and PgStore test helpers.
- [x] `with_rpc_admission(usize, Duration, Duration)` (`cluster/local.rs:158-164`)
  — adjacent same-typed values. Fixed by removing the raw-triple
  `LocalUnixStorageNodeClientConfig::with_rpc_admission` and
  `with_rpc_admission_from_runtime_node_route` constructors, moving
  `LocalUnixStorageNodeClientAdmissionSettings` next to the Unix RPC admission
  implementation, and making both `LocalUnixStorageNodeClientConfig` and
  `UnixStorageNodeClient` accept the settings object. The settings object is now
  constructed with named fields, so callers name `rpc_admission_limit`,
  `rpc_admission_wait_timeout`, and `rpc_control_admission_wait_timeout`.
- [x] Metrics render site pairs ~100+ name strings positionally with values
  in one giant `concat!` (`server-http/src/http/serve.rs:1466+`); tests check
  presence, not values, so a transposition mislabels metrics silently. Fixed by
  adding `MetricsSnapshot::iter_named()` as the fixed-metric export contract,
  rendering the debug endpoint from that iterator, adding value-binding tests
  for representative fields. Follow-up W6 removed the stale
  `bucket_lock_wait_exceeded_total` export when the obsolete bucket stripe lock
  was deleted.

### P5. Canonical tokens duplicated across crates / stringly-typed dispatch

- [x] Observability dimension keys are `&'static str` matched with silent
  `_ => {}` fallthrough (`lib.rs:1764-1790, 2317-2331, 3111-3125`):
  stream-upload `phase`, storage-rpc `admission_class`, background-work
  `event`. Live instance still live at re-review: server-http emits phase
  `"finalize_started"` (`serve.rs:4251`) which matches no arm — counter
  silently never updates. Fixed by adding closed observability enums for
  `StreamUploadPhase`, `StorageRpcAdmissionClass`, `BackgroundWorkClass`, and
  `BackgroundWorkAdmissionEvent`, updating callers to pass those enums instead
  of raw strings, matching counters exhaustively, moving storage RPC admission
  onto the observability class, and adding
  `stream_upload_finalize_started_total`.
- [x] `VersionId` has `Display` in s3-types but its inverse parser lives in
  server-http and is lossier (`mod.rs:128-137` accepted `versionId=0` as the
  null version via `from_u64(0)`, `s3-types/lib.rs:493-497`, which Display
  (:525) never produces). Fixed by adding `FromStr` for `VersionId` in
  s3-types, accepting `"null"` and nonzero decimal ids only, and making
  server-http map parse failures to the AWS-pinned `InvalidArgument` shape.
  AWS-facing coverage pins `versionId=0`, non-canonical `versionId=01`, and
  `version-id-marker=0` as `<Message>Invalid version id specified</Message>`
  with the supplied invalid `<ArgumentValue>`. DeleteObjects XML is explicitly
  different: AWS accepts an invalid `<VersionId>` token at request level and
  returns HTTP 200 with a per-object `NoSuchVersion` entry that echoes the raw
  token, so the local `DeleteObjects` path preserves invalid raw version IDs in
  `DeleteErrorVersionId` instead of reusing the top-level query parser error.
- [x] `BucketNamespace::as_header_value` had no parse counterpart —
  server-http matched `"global"`/`"account-regional"` literals
  (mod.rs:506-507 vs s3-types lib.rs:250-254). BucketNamespace is fixed:
  s3-types now implements `FromStr`, round-trips `as_header_value()`, and
  server-http delegates header parsing to the enum while preserving the
  missing-header default.
- [x] `BucketVersioningState` had `from_u8` but no wire-status parse/render
  API; `"Enabled"`/`"Suspended"` were literals in server-http XML parse/render.
  Fixed with `BucketVersioningState::as_s3_status()` and `FromStr`, preserving
  the important S3 shape that `Disabled` has no `<Status>` value.
- [x] auth helper triplication: `hex_encode` ×3 (`sigv4.rs:244`,
  `canonical.rs:442`, `request.rs:672`), `percent_decode` ×2
  (`canonical.rs:176`, `request.rs:642`), `hex_val` ×2 (`canonical.rs:194`,
  `request.rs:663`); and `server-core/src/sse.rs:735` re-implements
  `constant_time_eq` verbatim instead of using `auth::constant_time_eq`
  (`auth/src/lib.rs:43`). Fixed by adding a private `auth::encoding` module
  for lowercase hex encoding, percent decoding, and hex-nibble parsing, then
  routing canonical request construction, presigned request parsing, and SigV4
  signature formatting through it. SSE-C validator comparisons now call
  `auth::constant_time_eq` directly.

### P6. Error-type islands and wrong-blame mappings

- [x] Stringly-typed errors in otherwise fully-typed crates:
  `AclGrants::parse -> Result<_, String>` was fixed with typed
  `AclGrantsParseError` variants that preserve line-aware diagnostics while
  existing storage/RPC callers keep their outer error mapping;
  `PgTopology::new -> Result<_, &'static str>` was fixed with
  `PgTopologyError::Empty`;
  `SseCustomerObjectState::decode`/`SseS3ObjectState::decode`/
  `ObjectEncryption::decode -> Result<_, String>` was fixed by adding
  `ObjectEncryptionDecodeError` typed variants for persisted encryption-state
  shape/version/length errors; `RawChecksum::new`/`ChecksumBytes::new ->
  Result<_, &'static str>` was fixed with typed `RawChecksumError` and
  `ChecksumBytesError` variants, with storage/RPC/server boundaries preserving
  their existing outer classifications; `PutLiveObjectReq::validate ->
  Result<(), &'static str>` was fixed with typed `PutLiveObjectValidationError`
  variants for the ETag/layout consistency matrix;
  `SseCustomerValidatorConfig::from_base64`/`ManagedWrappingKeyConfig::
  from_base64 -> Result<_, String>` was fixed with typed config parse errors
  while preserving the existing startup `Display` messages.
- [x] Invalid: `parse_bucket_policy` does not need to enforce
  `MAX_BUCKET_POLICY_BYTES` on raw input. AWS-facing coverage pins that
  `PutBucketPolicy` accepts pretty/raw JSON over 20 KiB when the normalized
  stored policy is under the limit, and rejects oversized normalized policy.
  Keep the size check at the `PutBucketPolicy` admission/storage boundary,
  after parse, resource-scope validation, and condition validation; do not add
  a raw-size parser check unless an AWS raw body cap is discovered separately.

## Smaller items

### Placement / determinism

- [ ] **No cross-version placement pin.** Placement is recomputed at read
  time (`storage/src/cluster.rs:8146, 9257, 9768, 10507`); `rapidhash = "4"`
  floats on semver; any drift in hash/Unit53/log tables silently strands every
  existing shard. Fix: golden-vector test (fixed cluster + keys → expected
  NodeId sequences, committed) in the placement crate.
- [ ] **Deterministic-log corpus does not cover all table indices.**
  Substantially addressed since the review: generator/check scripts are now
  checked in (`scripts/placement-log-core-math` emit/check against a local
  CORE-MATH checkout, `scripts/check-log-hotspots`) and
  `deterministic_log_tables.rs:1-5` documents exact upstream provenance.
  Remaining gap: the committed corpus
  (`crates/placement/testdata/log_u53_reference.tsv`) is still ~30 rows,
  touching ≤30 of 363 fast-path table indices; a corrupted
  `INVERSE[i]`/`LOG_INV[i]` pair still yields silently wrong but
  self-consistent scores. Fix: extend the corpus to cover every table index
  (~400 rows, mechanical via the checked-in generator).
- [ ] `rack_cap` doc says "default max = m" but m=0 makes every placement
  fail with `ConstraintUnsatisfiable` (`constraint.rs:76-84`,
  demonstrated by placer.rs test). Document `max >= 1` or reject 0.
- [ ] Documented zero-alloc contract is false for keys > 516 bytes
  (`hash.rs:8` STACK_KEY_LIMIT, heap `Vec` fallback ~:25-29; `placer.rs:98`
  still documents "no heap allocation"; zero-alloc tests only use short
  keys). Document the limit at `place()` or pre-hash long keys.
- [ ] Module-visibility mix — partially reduced at re-review (placer/hash/
  deterministic_log are now private): cluster/config/constraint/topology
  still have dual paths (lib.rs:1-13); `MAX_SHARDS` load-bearing but
  `pub(crate)` (lib.rs:17); `PlacementError` variants carry raw `u32` despite
  `NodeId` existing (config.rs:44, 47).

### Observability

- [ ] Raw inc/dec counter pairs are pub where RAII guards should be:
  `emit_background_work_admission_event` wraps the stored atomic to
  `u64::MAX` on unpaired `"finished"` (`lib.rs:3115-3123`; same for
  `storage_rpc_admission_class_acquired/_released`, lib.rs:1719, 1726);
  correctness currently rests on consumer `Drop` impls. Expose guards (as
  already done for `inflight_requests_guard`, lib.rs:1708) and make the raw
  pair non-pub.
- [ ] `configure`/`configure_with_options` silently no-op on second call
  (bool discarded by sole caller, s3-tests/server.rs:406); lazily-created
  `TRACE_SINK` means late configure leaves earlier lines in the env sink
  (lib.rs:3281-3299). Log rejected re-configure; document call-before-first-
  event.
- [ ] Dimension tables silently stop recording new label combinations at
  capacity 512 — grew at re-review: now 14 tables share the silent cap (was
  ~9), including the new checkpoint-record-error and background-work
  dimensions. Add a `dimension_overflow_total` counter.
- [ ] Three context conventions in one API (explicit `&TraceContext` vs
  thread-local with silent flight-record skip, e.g. lib.rs:3127-3130, vs
  plain `event()` with no flight record, e.g. lib.rs:3097). Pick one; always
  write the flight record (crash-dump channel).
- [x] `emit_metadata_command_checkpoint_record_error` was the only error
  emitter without a counter. Fixed incidentally since the review: it now
  increments a labeled dimension counter
  (`increment_metadata_command_checkpoint_record_error_dimension`,
  lib.rs:761, called at lib.rs:3091). No plain `*_TOTAL`, but the substance
  is addressed.
- [ ] Idiom: all `emit_*` return an always-discarded `bool`; µs fields mix
  `u128`/`u64` with `saturating_u128_to_u64` sprinkled at use sites;
  `MetricsSnapshot` is still `Copy`, passed by value, and has grown more
  fields since the review (lib.rs:1528-1529); 4,866-line single file with
  ~2000 lines of doubled emit boilerplate whose format strings have already
  drifted once (`emit_reclaim_queue_action`).

### Storage misc

- [x] Metadata command payloads are inconsistent in carrying
  `BucketWriteReservationProof`: most object mutations embed it, but
  `AppendStreamSegment`, `DeleteCompletedMultipartUpload`,
  `AdvanceCompletedMultipartUploadSequence` carry none and `AbortStreamUpload`
  only an `Option` (`metadata_command.rs:692-1100`). Add a one-line doc
  comment per command stating which admission authority covers it.

  Resolution: documented the admission authority on the proofless/optional
  metadata commands. `AppendStreamSegment` and `AbortStreamUpload` are
  authorized by the stream session row, `DeleteCompletedMultipartUpload` by
  exact completed-upload row identity, and
  `AdvanceCompletedMultipartUploadSequence` by a CompleteMultipartUpload bucket
  write reservation proof at command-build time. The sequence command itself
  remains proofless because it does not release or own the bucket reservation,
  but local and Unix command-build requests now carry and validate the proof,
  including `operation_kind == "complete-multipart-upload"` and the expected
  object target context, before allocating the completion order.
- [ ] `ReserveObjectGenerationCommand::matches_request` (renamed from
  `ReserveObjectVersionCommand` since the review; finding applies verbatim)
  has a `generation_id` field but `matches_request` ignores it, matching on
  `reservation_id` (`metadata_command.rs:602, 627-634`) while
  `DeleteObjectVersionCommand::matches_request` compares `version_id`
  (:854-861); safe only under the coordinator's version-allocation-under-lock
  invariant (guides/object-concurrency.md #7) which lives in another crate.
  Document the cross-crate assumption at the impl.
- [ ] Lock-poisoning policy split three ways (recover / panic / panic):
  cluster.rs hook installers `.lock().unwrap()` (:671, :688, :705, :722,
  :4868...) vs `into_inner` recoveries elsewhere in cluster.rs, and
  storage_node_server.rs panics. Also server-http `mod.rs:4548` unwraps where
  server-core deliberately recovers (`coordinator.rs:78`) — one panic while
  holding that write guard poisons the session. Pick one policy (e.g. a
  `lock_recovering` helper).
- [ ] `wall_time_millis` silently returns 0 pre-epoch (`clock.rs:57-63`) —
  flows into created_at/lease math as 1970 (compare A3); `with_time_override`
  (clock.rs:19) is exported without test gating. Gate behind `test-hooks`.
- [ ] Wildcard re-exports `pub use s3_types::lifecycle::*` and
  `pub use types::*` (lib.rs:86, 95) make the crate surface unauditable.
  Enumerate.
- [ ] Panics reachable from pub API worth restructuring or doc-noting:
  checkpoint-proof `.expect()` in the peering import path
  (cluster.rs:3295, 3313 — the code touched by "Close Raft checkpoint capture
  race"); deliberate fail-fast `panic!` on invalid snapshots
  (control_plane.rs:2347, 3667, 4197) is fine but undocumented while
  `clippy::missing_panics_doc` is allowed crate-wide.
- [x] `unwrap`/`unreachable!` in EC reconstruction read/backfill paths. Valid
  but narrower than the original "~48 sites in cluster.rs" wording: the
  production issue was concentrated in placed-segment EC reconstruction and
  backfill assembly. These paths now convert inconsistent local recovery
  bookkeeping into `PayloadShardSetMismatch` instead of panicking, including
  missing reconstructed source segments, missing present-shard payloads, missing
  reconstructed data shards, and out-of-range reconstructed payload slices.

### Test-only footguns left pub

- [ ] server-http response builders that stamp the literal `"request-id"`
  into responses: `TEST_REQUEST_ID` (`response.rs:25` — only `TEST_HOST_ID`
  is `#[cfg(test)]`), `precondition_failed` (:1674), `forbidden` (:2144),
  `error` (:2165). Mark `#[cfg(test)]` or delete in favor of `_with_ids` +
  `WireResponseIds::for_test()`.
- [x] auth: `StreamingSigningContext`/`AuthContext` derive `PartialEq, Eq`
  over key material (request.rs:53, 80) — non-constant-time `==` on secrets;
  only tests use it. Fixed by removing equality derives from both types.
- [ ] `AuthContext` allows invalid anonymous/authenticated field combinations;
  an enum would remove the `Option`s (request.rs:82-89; consumers unwrap ad
  hoc).
- [ ] Minor idiom: `clippy::must_use_candidate` globally allowed in
  server-core while hand-annotating (the ec `#[must_use]` gap is largely
  moot since the P3 privatization added annotated accessors); infallible
  `bucket_request` returns `Result` (mod.rs:243-253);
  `xml_escape`/`xml_unescape` asymmetric on `'` (lifecycle.rs unescape
  handles `&apos;`, escape never emits it; same in server-http xml.rs);
  dead `MAX_PRINCIPAL_LEN` (s3-types lib.rs:11); `ChecksumHasher` derives
  nothing (hash.rs:8) while the CRC hashers derive Clone+Copy+Debug
  (crc32.rs:136), and `finalize` receiver semantics differ within the crate;
  `RawChecksum::MAX_LEN` duplicated privately (types.rs:295 vs 337);
  `derive_signing_key` copies the secret into a transient heap String with
  no zeroization (sigv4.rs:137); streaming request types disagree on owned
  vs borrowed bucket/key (`request_types.rs:808-830`).

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
- `control_plane.rs` implementation bodies (pub surface and peering
  apply/publish paths only), `peering.rs`, `storage_rpc.rs` internals
  (pub(crate)). The raft WAL surface in `control_plane_raft.rs` got a
  pattern-class pass at the 2026-07-05 re-review (results: RR15); its
  implementation bodies remain unreviewed.
- auth `condition_op.rs`/`condition_key.rs` per-operator bodies (dispatch
  verified only).
- ~~AWS-behavior claims in H2 and H6~~ — resolved: both were AWS-pinned
  during the fix work.
- Deterministic-log DInt64 arithmetic not verified bit-level against upstream
  CORE-MATH C (the checked-in `scripts/placement-log-core-math` check covers
  the corpus rows only).
