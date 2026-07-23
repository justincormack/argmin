# AWS Multipart Oracle Hardening Plan

## Goal

Harden multipart S3 behavior by deriving ambiguous request, lifecycle, and error
precedence rules from AWS and pinning them in tests that run unchanged against
AWS and the local server.

AWS responses are the authority. Documentation may suggest probes, but does not
decide expected behavior. Tests must not inspect the endpoint or select different
assertions for AWS and local execution.

## Workflow

For each matrix:

1. Construct fixtures with positive canaries so an error cannot pass because the
   credentials, bucket, upload, or part setup is broken.
2. Probe AWS with raw signed requests when the SDK cannot preserve the required
   malformed or duplicate wire shape.
3. Replace diagnostics with exact shared assertions, then run the same test
   locally.
4. If local behavior differs, understand the AWS ordering rule before changing
   implementation code. Assert both response behavior and externally visible
   state after failed mutations.

## Coverage

### 1. Upload identity, authorization, and parsing precedence

- [x] Pin the exact overlong nonexistent upload-ID response for the primary
  principal across UploadPart, CompleteMultipartUpload, ListParts, and
  AbortMultipartUpload.
- [x] Cross active and overlong nonexistent upload IDs with the alternate
  principal for all four operations. Require `AccessDenied` for the active ID
  before accepting `NoSuchUpload` for the malformed ID, and prove denied writes
  stored no parts.
- [x] Pin CompleteMultipartUpload ordering across active/nonexistent upload IDs,
  well-formed/malformed XML, and primary/alternate principals.
- [x] Cover missing/empty, duplicate, percent-encoded, and wrong-key upload IDs
  across UploadPart, CompleteMultipartUpload, ListParts, and
  AbortMultipartUpload, including exact error fields, first-value precedence,
  key binding, and final upload/part/object state. An absent `uploadId` only
  identifies UploadPart when `partNumber` is present; absent IDs on POST, GET,
  and DELETE select the corresponding non-multipart object operation.

### 2. Completion validation and atomic publication

- [x] Prove `InvalidPart` leaves the upload and parts usable, publishes no object,
  and permits a corrected completion.
- [x] Prove rejected completion over an existing object leaves the old ETag and
  bytes visible until a valid completion atomically replaces it.
- [x] Apply the same state assertions to `InvalidPartOrder`, `EntityTooSmall`,
  checksum failures, expected-size failures, conditional failures, and malformed
  completion bodies. Raw response-shape tests explicitly accept AWS's documented
  CompleteMultipartUpload behavior where a processing failure may use either
  its ordinary error status or an error body embedded in HTTP 200. SDK-based
  tests already handle both forms; the local server uses ordinary error statuses.
- [x] Determine AWS precedence when several completion errors coexist; include
  upload existence, XML shape, part order, missing part, ETag, part size,
  checksum, condition, and expected object size. The oracle matrix also pins
  header-shape validation and proves that aggregate-checksum precedence is not
  a total linear order: it beats a missing part, while an ETag mismatch beats
  the aggregate checksum. AWS's repeatable `InternalError` for a valid,
  checksum-configured but non-consecutive part list is covered under the
  documented compatibility policy that permits Argmin's `InvalidRequest`.
- [x] Probe retries after successful completion and after abort, including object
  overwrite/delete and bucket delete/recreate histories. AWS replays the exact
  successful completion while its published object version remains retained,
  but returns `NoSuchUpload` for a changed manifest or after an unversioned
  overwrite/delete.
  Abort is idempotent for completed and aborted upload IDs and does not consume
  a completed upload's exact replay. These terminal retries remain valid until
  bucket deletion/recreation, which makes every operation on the old IDs return
  `NoSuchUpload`.
- [x] Extend the terminal-retry oracle matrix to versioned buckets. A completed
  version remains the exact-replay target across later versions and delete
  markers, including removal of the delete marker, and ceases to replay only
  when that completed version is explicitly deleted. The replay returns the
  original ETag and `x-amz-version-id`.
- [x] Probe otherwise exact terminal completion retries with changed request
  conditions, aggregate-checksum headers, and `x-amz-mp-object-size`. AWS uses
  only the completion manifest as replay identity. It ignores syntactically
  valid matching or mismatching `If-Match`, aggregate-checksum, and expected-size
  values, and ignores `If-None-Match: *`; malformed values and unsupported
  specific `If-None-Match` values fail before replay. Successful terminal
  replays return the original ETag and version ID but omit checksum result
  fields, even when the original completion supplied aggregate-checksum and
  expected-size claims. The shared AWS/local matrix pins these response shapes
  and proves that neither object data, object versions, nor upload state mutate.
- [x] Probe the corresponding terminal-retry histories in a suspended bucket.
  A retained numbered completion continues to replay through suspension, null
  writes, a null delete marker and its removal, and a later null multipart
  completion. Deleting that numbered version ends only its replay. A completed
  null version replays only while that exact null object row remains: a later
  PUT, null delete marker, explicit null-version deletion, or later multipart
  completion ends the old replay, and removing the delete marker does not
  resurrect it. The shared matrix also pins the suspended delete marker's
  `x-amz-version-id: null` response, the same header on explicit null-version
  deletion, and the replacement/reclamation of the old null payload.
  Repeating an unversioned delete while that null delete marker is current
  remains immediately successful and returns the null marker response; local
  and Unix-client storage regressions pin the no-payload command path.
- [x] Probe terminal completion retries after the initiator's current
  `s3:PutObject` permission is removed and under an explicit deny, with positive
  policy canaries before each transition. AWS applies the current policy: an
  explicit deny rejects the replay, and removing it restores the replay. AWS
  also permits an exact replay by a same-account non-initiator that is currently
  granted `s3:PutObject`, in both ACL and bucket-owner-enforced buckets.
  Authenticated upload identity therefore establishes the target but does not
  replace current completion authorization.
- [x] Cross malformed completion XML and malformed expected-size headers with a
  completed upload whose published object has subsequently been
  overwritten. AWS still reports `MalformedXML` or `InvalidRequest` before
  `NoSuchUpload`: request-layer preflight recognizes an authenticated issued ID
  independently of whether a retained completion replay still exists. The full
  valid completion path performs the replay lookup and returns `NoSuchUpload`
  when no retained replay is applicable.
- [x] Replace terminal multipart tombstones with a storage model proportional to
  active uploads and live object versions, not historical multipart activity.
  Per-bucket retention limits are not acceptable: multipart is the ordinary
  object-write path, so even a fixed limit such as 10,000 records per bucket
  creates a large permanent metadata tax across many buckets.
  - Generate opaque authenticated upload IDs bound to the bucket, key, and
    bucket-incarnation generation. Include any initiator/owner claims required
    by the AWS authorization oracle, and define durable signing-key lifecycle
    and restart behavior. A mutated ID, an ID used with another key, or an ID
    from a deleted bucket incarnation must fail validation.
  - Keep exact-completion replay data with the object version produced by the
    completion: the authenticated upload identity, completion-manifest digest,
    and response fields not otherwise derivable from the object metadata.
    Replay succeeds only while the completed object version is retained and the
    manifest matches exactly. Request preflight authenticates the issued ID
    without requiring replay state, preserving AWS's XML/header validation
    precedence; the full path separately loads the retained replay and repeats
    the lookup to close deletion races.
  - For an authenticated issued ID with no applicable completion replay,
    CompleteMultipartUpload returns `NoSuchUpload`; AbortMultipartUpload remains
    idempotently successful after either completion or abort without retaining
    a terminal row. Perform authorization after authenticating the key-bound ID
    so the existing hidden-resource `AccessDenied` behavior remains possible.
  - Remove the existing bounded completed-upload tombstone table/path as well as
    the uncommitted completed/aborted extension. Add storage tests proving that
    completion, abort, overwrite, and deletion do not accumulate historical
    terminal rows, plus restart and bucket delete/recreate tests for upload-ID
    authentication and replay state. No pruning or cleanup mechanism may be
    required to maintain the storage bound.
  - Preserve the replicated cross-PG completion dependency with
    `AdvanceMultipartCompletionBarrier`. It advances one fixed-size scalar on
    the bucket row before object-PG publication and retains no upload identity
    or terminal history. Because the command contains no reservation identity,
    a retry first drains any barrier left pending by an earlier reservation and
    then advances a fresh barrier validated against its current reservation.
- [x] Once the tombstone-free implementation matches the shared AWS/local
  matrix, replace the compatibility-guide exception for terminal hidden-resource
  denial with the authenticated-ID and object-scoped replay invariants.

### 3. Part upload and copy boundaries

- [x] Pin UploadPart part-number parsing and limits at missing, empty,
  nonnumeric, signed, zero, 1, 10,000, 10,001, duplicate, and overflow values.
  Pin UploadPartCopy's shared numeric/duplicate rules and its distinct
  no-XML-declaration error shape. Assert the accepted first duplicate is the
  part stored and that missing `partNumber` publishes no object.
- [x] Pin zero-byte parts, 5 MiB minus one/exactly 5 MiB non-final parts, final
  part exceptions, overwritten parts, and maximum-part completion behavior.
  AWS applies the minimum only to the selected non-final parts and uses the
  latest upload for an overwritten part number. A lone part numbered 10,000
  completes successfully. A 10,000-entry manifest proceeds to part lookup,
  while 10,001 entries fail first with AWS's exact `InvalidArgument` response;
  the count boundary is tested without transferring 10,000 full-size parts.
- [x] Cross invalid part numbers/ranges/checksums with invalid upload IDs and
  unauthorized principals to establish error precedence and non-publication.
  AWS first resolves whether the upload ID names an active upload, ahead of
  malformed Content-MD5/checksum and incomplete SSE-C headers. For an active
  upload, syntactically or semantically invalid part numbers and malformed copy
  ranges precede current-policy authorization; Content-MD5 mismatch and copy
  ranges beyond the source size follow authorization. The shared matrix
  includes a successful copy canary and proves every rejected request leaves
  the target upload empty and publishes no object.
- [x] Verify failed UploadPart and UploadPartCopy replacements, plus an
  interrupted UploadPart request body, neither replace a prior valid part nor
  become visible in ListParts/completion. AWS and local retain the original
  part's ETag and size after a checksum failure, a mid-body transport failure,
  and an UploadPartCopy source-condition failure; completion with that original
  ETag publishes its original bytes. UploadPartCopy has no request body to
  interrupt. Disconnecting while awaiting its response does not establish that
  the server-side copy failed, so permitted raced outcomes belong to the
  concurrency matrix below rather than this failed-request invariant.

### 4. Listing and marker behavior

- [x] Expand ListParts limits, markers, ordering, overwritten-part metadata,
  checksum fields, and active/terminal upload states.
- [x] Expand ListMultipartUploads ordering and pagination across same-key uploads,
  prefixes, delimiters, encoding, duplicate timestamps, completion, abort, and
  concurrent state changes.
  Same-key creation ordering, two-marker continuation, non-truncated next
  markers, prefix/encoding behavior, duplicate-timestamp tie-breaking, and
  completion/abort removal and delimiter/common-prefix pagination are covered.
  The lifecycle matrix pins continuation after the exact marker is aborted,
  creation before and after that stale marker, creation with the same key, and
  completion/abort removal between pages. Authenticated upload IDs carry their
  durable creation position, consisting of the PG metadata command's cluster
  epoch and within-epoch log index, so the stale marker resumes at its former
  position across epoch transitions without retaining a per-key counter or
  terminal upload metadata. The minimal
  abort/recreate case proves a replacement remains visible even when no other
  same-key upload preserves the old object generation.
- [x] Pin malformed and duplicate listing parameters and authorization/error
  precedence with positive list canaries. ListParts and
  ListMultipartUploads treat empty numeric parameters as omitted, accept an
  explicit plus sign, reject values outside the signed 32-bit range, clamp
  valid limits above 1,000, and select the first duplicate value. Malformed
  `max-parts` precedes `part-number-marker`, which precedes upload lookup;
  malformed `max-uploads` precedes `encoding-type`, which precedes an
  effective `upload-id-marker`. These validation failures precede current
  authorization. ListMultipartUploads ignores `upload-id-marker` unless a
  nonempty `key-marker` is also present, and first-value selection is pinned
  for key markers, prefixes, and delimiters.

### 5. Lifecycle and concurrency

- [x] Probe complete/abort races, UploadPart/abort races, UploadPart/complete
  races, simultaneous completions, and concurrent uploads to the same key.
- [x] Assert only AWS-permitted outcomes, plus final object bytes, upload
  visibility, part visibility, and retry behavior for every basic outcome.
  Complete versus abort is serializable. When completion wins, the raced abort
  may either succeed or return `NoSuchUpload`; when abort wins, it succeeds and
  completion returns `NoSuchUpload`. Sequential abort after completion remains
  idempotently successful. Simultaneous identical completions are idempotent
  and all return the same result. Different manifests for one
  upload may both return success, although only the published manifest remains
  replayable; a competing call may instead return `NoSuchUpload`. Distinct
  uploads to one key both complete successfully, with last-writer object state
  and replay retained only for the current unversioned object. UploadPart
  replacement versus completion is serializable: either completion publishes
  the original part and replacement returns `NoSuchUpload`, or replacement is
  retained and completion of the old ETag returns `InvalidPart`.
  Deterministic coordinator regressions pin completion races after
  authorization, after snapshot lookup, and before commit. Completion restarts
  terminal-transition races once and uses a separate elapsed-time
  stale-snapshot retry budget so fast replacement/contention cannot exhaust a
  fixed attempt count before it has had time to settle. Exhausting that budget
  under valid same-ETag part replacement returns retryable
  `OperationAborted`; a deterministic regression proves that it publishes no
  object and leaves the upload and selected part intact. Object-version and
  generation reservation, direct PUT publication, object reads, reservation
  release, and stream-segment append likewise use elapsed-time contention
  budgets rather than scheduling-sensitive attempt counts. Once stream-append
  metadata-command ownership becomes ambiguous, cleanup fences new command
  installation and checks the whole acting set before deleting the staged
  payload. This preserves shard keys published by an idempotent reissue even
  when its pending slot has already been cleared, while still deleting
  unreferenced shards after unrelated contention. Deterministic
  visible-pending, cleared-slot collision, unrelated-pending duplicate, and
  unrelated retry-exhaustion and failed LogConflict-drain coverage pins both
  sides of that rule. Multipart
  management lookup drains a concurrent durable terminal command before
  classification, preserving AWS's idempotent abort result when completion
  wins the race. AWS can also return transient `OperationAborted` after its SDK
  retry allowance is exhausted when versioned completions race each other or a
  versioned delete. Those tests release the first requests together, then
  retry only `OperationAborted` under the shared elapsed-time test budget; the
  exact final version IDs, retained bytes, single delete marker, and replay
  assertions still detect duplicate or lost mutations.
- [x] Cover versioned buckets, delete markers, conditional completion, and object
  replacement without relying on timing-sensitive exact boundaries. Concurrent
  completions of distinct uploads in a versioned bucket both publish retained,
  independently replayable versions. A completion racing a versioned delete
  retains both the completed version and delete marker; only their latest state
  varies. Conditional completion is also bound to the current-object identity
  observed at multipart initiation: normal conditions are evaluated against the
  current object first, but a condition that would otherwise pass returns
  `409 ConditionalRequestConflict` when an intervening replacement or deletion
  changed that identity. The old upload remains listable with its parts, while a
  newly initiated upload can complete under the new current-object condition.
  In a versioned bucket, deleting an intervening replacement so the exact
  initiation-time version becomes current again permits conditional completion;
  the rule is current identity, not merely whether an intervening write occurred.
  In a suspended bucket, replacing a current null delete marker with another
  null marker produces a different identity and returns the same conflict. Local
  identity therefore includes the marker's durable write sequence rather than
  relying on its nullable version ID and millisecond timestamp.
- [x] Cover multipart copy source changes without relying on timing-sensitive
  exact boundaries. UploadPartCopy racing an unversioned source replacement
  copies exactly one complete source version: its result ETag, listed part, and
  completed destination bytes all identify either the old or new object, never
  a mixture. Racing source deletion either copies the old version successfully
  or returns `NoSuchKey` without storing a part. In the rejected branch the
  upload remains usable by a successful copy retry after the source is restored.

### 6. Cross-feature multipart contracts

- [x] Inventory and fill oracle gaps for checksums, metadata, tagging, ACL/BOE,
  SSE-C, managed encryption rejection, expected owner, object lock, and
  lifecycle abort headers. Existing endpoint-independent suites already cover
  checksum creation/part/copy/completion contracts, initiation-time metadata
  and tags, ACL and BOE ownership, SSE-C key propagation and copy, illegal
  managed-encryption follow-on headers, the expected-owner operation matrix,
  explicit/default Object Lock state, and lifecycle header presence and
  filtering. Requester Pays is excluded because the product does not implement
  that wider billing feature and the compatibility guide already records the
  unsupported wire surface.
- [x] Keep feature-specific setup in the owning shared `s3-tests` suites and run
  identical assertions on AWS and local endpoints. The inventory found one
  lifecycle gap: after a replacement lifecycle rule has visibly converged,
  AWS recalculates an existing upload's ListParts abort rule and date using the
  current rule and the upload's original initiation time. A positive canary
  upload distinguishes this from ordinary lifecycle control-plane propagation;
  the same oracle now passes locally.

### 7. Conditional operations beyond multipart completion

Multipart completion exposed a broader class of conditional-operation risks:
the condition, authorization, and mutation must refer to a coherent object
state, and contention responses must be derived from AWS rather than inferred
from documentation. Extend the oracle pass to PutObject, CopyObject,
UploadPartCopy, DeleteObject, DeleteObjects, GetObject, and HeadObject. Keep all
public tests endpoint-independent and do not retry the first raced conditional
request, because retrying `OperationAborted` would hide AWS's original 409/412
choice.

- [x] Fill the static CopyObject destination matrix. Pin `If-None-Match: *`
  against missing and existing destinations, then cover `If-Match` and
  `If-None-Match: *` across current live objects, current delete markers,
  enabled versioning, and suspended null versions. Every rejection must prove
  that the destination bytes, ETag, versions, and source remain unchanged.
  The shared AWS/local cases now pin success for a missing destination and
  `412 PreconditionFailed` with unchanged ETag and bytes for an existing
  destination. In a versioned bucket, rejection over a current live version
  creates no new version; a current delete marker makes destination `If-Match`
  return `404 NoSuchKey`, while `If-None-Match: *` succeeds and publishes a new
  current version without removing the retained object version or marker.
  Matching `If-Match` over a versioned live destination publishes a distinct
  latest version and retains the exact prior version and bytes. In a suspended
  bucket, rejected conditions preserve the current null live object or null
  delete marker and the older numbered version. `If-None-Match: *` over the
  null marker replaces it with a null live copy; matching `If-Match` then
  replaces that null live row without accumulating null history, while the
  numbered version remains readable. AWS omits `x-amz-version-id` from the
  suspended PutObject setup response but returns `x-amz-version-id: null` from
  suspended CopyObject and every GetObject/HeadObject variant, including range
  and `partNumber` requests. The local response paths now carry the bucket
  versioning state so they can match this operation-specific distinction. The
  same oracle also pins that `partNumber=1` over a non-multipart object omits
  `x-amz-mp-parts-count`; multipart objects continue to return their count.
- [x] Fill CopyObject and UploadPartCopy source-condition coverage. Cross the
  four source conditional families individually and in AWS's combined-header
  precedence pairs, including malformed dates, missing/current-delete-marker
  sources, explicit source versions, and source replacement or deletion while
  a copy is in progress. A success must copy one coherent source snapshot; a
  failure must publish no destination or part. The combined ETag/date pairs are
  now pinned for both operations: a present `If-Match` controls independently
  of `If-Unmodified-Since`, and a present `If-None-Match` controls independently
  of `If-Modified-Since`. The full truth table proves successful copies contain
  the source bytes and ETag, while each 412 publishes neither a destination nor
  a multipart part. A second cross-operation matrix pins source resolution
  ahead of all four condition families: missing and current-delete-marker
  sources return `404 NoSuchKey`, an explicitly selected delete-marker version
  returns `400 InvalidRequest`, and conditions on an explicitly selected live
  version use that version's ETag and timestamp rather than the current source
  state. Successful explicit-version copies return
  `x-amz-copy-source-version-id`; local CopyObject and UploadPartCopy responses
  now preserve that selected source identity. A separate source-version header
  oracle pins the complete rule for both operations: an implicit current
  numbered version returns its ID; an implicit null version in a never-versioned
  or suspended bucket omits the header; and explicitly selecting
  `versionId=null` returns `null` in both bucket states. Exact source version and
  marker history is unchanged, and every rejection publishes neither a
  destination nor a part. Both malformed source date headers are ignored by
  both operations, with the successful destination or part state verified.
  AWS-facing CopyObject probes establish the permitted replacement and
  deletion orderings: replacement publishes exactly one complete old or new
  source snapshot, while deletion either publishes the complete old snapshot
  or returns `404 NoSuchKey` without a destination. These match the equivalent
  UploadPartCopy source-race outcomes in the multipart concurrency matrix. The
  staggered AWS probes do not themselves prove that the operations overlap.
  Deterministic coordinator regressions now pause CopyObject after its source
  snapshot, complete replacement or deletion, and then require the old body,
  ETag, content type, and user metadata on resume. The overwrite case also
  verifies that the current source contains the distinct replacement state.
  Source authorization now returns the exact snapshot with a broad generation
  lease; CopyObject and UploadPartCopy retain it through condition, encryption,
  and metadata processing and hand it off to the shard-specific read lease
  without an unleased reclamation interval. The storage regression also pins
  retry when the authorization subject changes before exact snapshot loading.
  Broad leases are session-owned on the actual storage nodes rather than on
  frontend topology placeholders; an installed-Unix A-to-B runtime-map refresh
  regression proves that reclaim through B remains deferred until A releases.
  Their long-lived Unix sessions consume one aggregate RPC admission limit and
  its shared non-control budget. All lease acquisition stops one slot short of
  that budget to preserve narrow-to-read progress; broad acquisition stops one
  additional slot earlier so new broad work cannot consume the broad-to-narrow
  transition slot. Minimum-limit saturation coverage performs the real
  one-at-a-time handoff: acquire a shard-scoped successor, release its broad
  predecessor, then reuse the transition slot. Direct narrow-lease saturation
  coverage also proves an ordinary read handle can still open. Short
  reclaim/count control RPCs progress, and aggregate live sessions never exceed
  the configured limit.
- [x] Probe conditional PutObject contention for both the direct and streamed
  paths, including aws-chunked requests. Cover simultaneous
  `If-None-Match: *` creates, competing `If-Match` overwrites, intervening
  different-ETag replacement, and same-ETag replacement with different
  metadata or tags. Record the first AWS response without the OperationAborted
  retry helper, then assert permitted 409/412 outcomes, final object bytes and
  metadata, retry behavior, and absence of partial writes. Coordinated
  first-body-poll probes start two ordinary PutObject clients together; the
  multi-segment variant paces both bodies to retain likely overlap. AWS and
  local publish exactly one complete writer, with the loser returning either
  `409 ConditionalRequestConflict` or `412 PreconditionFailed`; a settled
  retry returns 412. The body-poll notification is not a transport handoff
  acknowledgement, so the earlier intervening-replacement cases establish
  outcomes but not their internal check point. Replacing destination state
  before a request is handled with identical bytes but different metadata and
  tags retains the ETag and permits the conditional PUT.

  One-off raw SigV4 probes now establish the stronger late conditional rule.
  A bucket-owner `If-Match` PUT wrote and flushed 64 MiB of a 128 MiB body
  before a different-ETag replacement; AWS returned
  `412 PreconditionFailed`, published none of the conditional body, and
  retained the replacement. In an ObjectWriter bucket, a bucket-owner request
  crossed the same boundary before the alternate account installed an
  identical-byte, same-ETag private replacement. No response arrived during
  the two-second post-replacement window; after the remaining body was sent,
  AWS returned `409 ConditionalRequestConflict`. Direct inspection confirmed
  the retained object was still the 27-byte alternate-owned replacement with
  its original ETag and sole-owner ACL. Thus authorization may latch at ingress
  while conditional publication remains tied to the later atomic destination
  state: a now-false ETag condition returns 412, while a same-ETag intervening
  generation can still conflict with 409. Signed aws-chunked success,
  stale-condition failure, and non-mutation pin the same static semantics
  without introducing a transport-specific rule. The 128 MiB probes remain
  documentary.
- [x] Audit the earlier blanket authorization-at-entry assumption anywhere a
  long-running operation can observe mutable authorization state. Keep bucket
  control-plane state (policy, ownership controls, public-access settings, and
  bucket tags) separate from strongly consistent object owner, ACL, and tag
  state. For each candidate, use AWS to establish which snapshot or
  linearization point controls before changing the local authorization token;
  do not infer a general commit-time reauthorization rule from conditional
  PutObject. Fold the concrete CopyObject and delete cases below into this
  audit, and include object ACL/tag mutations where the operation exposes a
  meaningful race interval.

  Exploratory cross-account ObjectWriter oracles used positive
  ownership/access canaries and established two important AWS results. First,
  replacing a requester-owned object with an identical-byte private
  bucket-owner object while an `If-Match` PutObject body is in flight makes the
  Put return `403 AccessDenied`, preserves the replacement, and leaves a
  settled retry denied despite the unchanged ETag. Second, revoking the
  requester's bucket ACL write grant, waiting until fresh PUTs consistently
  return 403, and then resuming a request started while the grant existed also
  returns 403 and publishes nothing. An already-denied paused PUT did not
  expose its 403 until the body completed. Subsequent raw probes below narrow
  the first result to conditional `If-Match` PUT; it is not a general
  current-object authorization rule for unconditional PutObject. These
  ObjectWriter timing cases are documentary because a small flushed prefix
  does not establish the internal handoff, and accepting every resulting
  200/403/409 branch would not make a useful maintained regression. The bucket
  ACL result remains a separate authorization-source transition.

  The attempted shared commit-time implementation was removed because it also
  changed unresolved CopyObject and POST Object timing and treated current
  delete markers as existing objects during authorization. Keep the local
  entry-only model until the complete matrix supports operation-specific token
  capabilities and explicit live-object/delete-marker normalization.

  The corrected BOE PutObject oracle slice writes HTTP/1.1 headers and body
  bytes directly to TCP/TLS and waits for `flush()` before changing external
  state. The earlier SDK-body acknowledgement occurred before Hyper accepted
  the frame and did not prove transport delivery; none of the earlier timing
  conclusions rely on it now. With raw Authorization-header SigV4, AWS returns
  an existing `AccessDenied` only after the client has finished writing the
  exploratory 256 KiB and 4 MiB bodies. The same is true for anonymous private
  PUTs at those sizes. Local returned anonymous denial after flushed headers
  and signed denial after the flushed 64 KiB prefix. Response-stage and stable
  denial cases are no longer maintained because their size/buffering outcome
  is not itself a useful compatibility invariant.

  Corrected one-off 128 MiB probes did not require the complete body. For an
  anonymous request AWS closed with `403 AccessDenied` after the raw client had
  successfully written 16,984,136 of 134,217,728 bytes. A valid
  Authorization-header SigV4 request from the alternate principal, after its
  explicit policy denial was observed as stable, similarly closed with 403
  after 17,085,614 bytes. Those counts are the transport write points at which
  the client observed the close, not claimed AWS buffering thresholds. Their
  proximity makes an anonymous-only early-denial path unlikely. They prove
  that AWS can emit a denial before receiving a large authenticated body in
  full, but do not establish where authorization occurs internally. In
  particular, a bounded ingress buffer followed by a one-time authorization
  check on entry to the operation system is consistent with these results:
  small bodies could fit before handoff, while large bodies force an earlier
  handoff. The bandwidth-heavy cases remain documentary.

  Correct transport acknowledgement also removes the apparent mutable-policy
  asymmetry. After a signed prefix is flushed, allow -> stable explicit deny
  returned `403` for both exploratory sizes; stable explicit deny -> allow
  returns 200. A deny -> allow -> deny sandwich, with each state visible to
  canaries for 30 seconds and body bytes flushed throughout, returns 403.
  These outcomes show that the decisive authorization was not fixed before the
  flushed prefix and followed a later converged policy state in each probe.
  They do not place that decision at body completion or prove reauthorization.
  The flushed 64 KiB prefix may still have been held in a transport/ingress
  layer that is outside the operation's logical authorization entry point.

  A further one-off boundary probe strongly supports the later-handoff model.
  With the allow already stable, the signed client wrote and flushed 64 MiB of
  a 128 MiB body and observed no response. It then installed an explicit deny,
  kept the body active while fresh PutObject canaries observed that deny
  continuously for 30 seconds, and sent the remainder. AWS returned 200 and
  published the complete 128 MiB object. Combined with the 64 KiB-prefix case
  returning 403 and fixed denials becoming observable around 17 MiB, this
  supports a successful authorization decision being made and latched at a
  size-driven ingress handoff before the policy transition. It argues against
  authorization being deferred until commit or unconditionally repeated at
  commit. The observations still do not expose an exact internal threshold:
  client, TLS, and service buffering affect the byte counts, and AWS provides
  no handoff acknowledgement. Local remains entry-bound at its HTTP operation
  layer: it returns 200 for allow -> deny and the original 403 for deny ->
  allow.

  The maintained timing-dependent PUT coverage is deliberately narrower: one
  4 MiB allow -> deny case and one deny -> allow case. Each permits only 200 or
  403 because either authorization state can legitimately win the unobservable
  handoff race. A 200 must expose the exact complete body; a 403 must leave the
  key absent and carry an `AccessDenied` XML code, so a broken signature cannot
  satisfy the denial branch. The tests do not assert response stage. Their raw
  TCP/TLS request retains one configured deadline from connection setup
  through every later body write, flush, and response read; focused
  backpressure and non-responding-peer regressions prevent a timing oracle from
  hanging the test process. Duplicate sizes, stable denial stage probes, the
  deny -> allow -> deny sandwich, and the small-prefix ObjectWriter timing
  cases were removed; their observed AWS results remain documentary here.

  Strongly consistent object-state probes now establish a narrower GetObject
  rule without relying on control-plane convergence. AWS and local both allow
  an already-started streamed GetObject to return its complete original bytes
  after either its canonical-user READ ACL is replaced by a private ACL or its
  policy-authorizing `s3:ExistingObjectTag/security=allow` tag is replaced by
  `deny`. Each test reads the replacement ACL/tag back, verifies a fresh HEAD
  is denied, and only then drains the original response. This pins
  authorization to the established GetObject response rather than requiring
  continuing authorization throughout response streaming; it does not imply
  the same rule for writes or server-side copies.

  The documentary ObjectWriter matrix shows that conditional `If-Match` PUT
  differs from unconditional PutObject, but the ingress-buffer findings narrow
  what can be inferred about authorization timing. An alternate-account writer
  flushes a 64 KiB signed prefix against its own private object; replacing it
  with identical bytes and ETag under the bucket owner's private ACL before
  body completion makes AWS return `403 AccessDenied`. Reversing that
  transition also returns 403. However, a one-off attempt to extend the first
  request to a 64 MiB prefix had its connection closed before crossing that
  boundary, even before the replacement; the temporary helper did not retain
  the response status. That direction therefore failed to establish an allowed
  post-handoff start. The small-body 403 does not prove a current-object
  authorization recheck; it can represent an initial denial whose response was
  deferred while the body was buffered.

  Reversing principals gave the bucket owner's request an unquestionably
  allowed start and crossed the 64 MiB handoff before the alternate account
  installed the same-ETag private object. That request returned 409 after body
  completion, not 403, and left the alternate-owned replacement untouched.
  This proves a late conditional generation conflict but does not prove late
  ACL authorization. Do not implement current-object reauthorization from the
  earlier 403 pair. Local's differing small-body statuses remain useful
  observations, not evidence for the final capability model.

  This is not ordinary PutObject authorization against the current object's
  ACL. A settled alternate-account unconditional overwrite of a private
  bucket-owner object succeeds on AWS and local when bucket ACL WRITE permits
  it. A staged unconditional PUT that starts against an alternate-owned object,
  is paused while the bucket owner installs a private replacement, and then
  completes also succeeds and publishes an alternate-owned object on both.
  Therefore a general commit-time reauthorization based on current object
  owner/ACL would be incorrect. The established late behavior belongs to
  conditional mutation/conflict semantics, not yet to authorization.

  Repeat mutable bucket-policy cases for ObjectWriter rather than inferring
  them from the bucket ACL result. Cover direct PutObject, streamed PutObject,
  aws-chunked PutObject, and POST Object independently, including absent keys,
  live objects, and current delete markers. Assess CopyObject separately
  because it has no request body with which to establish a controllable race
  interval. Use object tag conditions only on AWS actions for which they are
  evaluable; notably, `s3:ExistingObjectTag/*` is policy-invalid for destination
  PutObject. Do not change commit authorization without an oracle result that
  identifies an operation's actual authorization points.

  The maintained ObjectWriter bucket-policy matrix now covers direct and
  streamed ordinary PutObject, signed aws-chunked PutObject, and POST Object
  over absent keys, live versions, and current delete markers. Each raw request
  writes and flushes a complete prefix (including a complete signed
  aws-chunked data chunk or multipart/form-data file prefix), continues making
  body progress while the replacement policy converges, and accepts only the
  operation's success status or `403 AccessDenied`. Success must publish the
  exact complete decoded/file body as one new current version without changing
  any prior version or marker identity; denial must leave the complete
  version history and visible baseline body unchanged. All staged bodies are
  completed concurrently before the slower state assertions so AWS cannot
  expire a later socket while an earlier case is being inspected.
  The ordinary streamed cases use
  `server_core::coordinator::INTERNAL_SEGMENT_SIZE + 1`, with compile-time
  assertions keeping the direct case at or below that shared threshold and the
  streamed case above it. This pins local coverage of promotion from the
  single-segment path into streaming storage and its separate finalization.

  AWS confirms that these outcomes follow the unobservable ingress handoff,
  not a destination-state-specific authorization rule. In one corrected plain
  run and the aws-chunked run, deny -> allow returned success for all three
  states and allow -> deny returned 403 for all three; an earlier corrected
  plain run mixed success and denial within deny -> allow. POST Object also
  mixed branches: the latest run returned 403 for absent/live and 204 for the
  current-marker case under deny -> allow, then 204 for absent/live and 403 for
  the marker case under allow -> deny. An earlier run selected a different
  branch for one of the same cases. Local consistently preserves its
  entry-bound model (initial allow succeeds; initial deny remains denied), and
  every local result is one of the AWS-permitted, fully state-checked branches.
  CopyObject was considered separately. It has no request body with which to
  hold the operation across control-plane convergence, and its server-side
  source read cannot be paced or acknowledged by the client.
- [x] Conclude that CopyObject destination-authorization timing is not usefully
  observable through the public API. A large source can only make the request
  probabilistically remain outstanding; it cannot establish whether
  destination authorization happened before, during, or after the source read.
  Any observed success or denial would remain compatible with several
  authorization models and would not justify changing the local implementation.
  Do not add an expensive timing-dependent oracle that cannot distinguish those
  models. Keep CopyObject entry-authorized locally unless a future independently
  controllable AWS behavior exposes new evidence.
- [x] Probe DeleteObject and DeleteObjects races against same-ETag and
  different-ETag replacement, current delete-marker insertion, enabled
  versioning, and suspended null replacement. Pin per-entry DeleteObjects
  results and final state. The local implementation already re-runs current
  object authorization and the ETag condition inside one storage callback;
  deterministic regressions must prove that invariant under replacement.

  Shared AWS/local races now cover both endpoints across unversioned,
  version-enabled, and suspended buckets. A different-ETag replacement leaves
  the complete replacement visible and permits the conditional delete to have
  serialized first, to return settled `PreconditionFailed`, or to return
  `ConditionalRequestConflict` when AWS detects the overlapping generation.
  Same-ETag unversioned replacement remains ETag-based: repeated AWS runs
  serialized the delete successfully, with final state determined by which
  successful mutation was last. Suspended null replacement exposes the
  stronger overlap branch: even identical bytes and ETag can return
  `ConditionalRequestConflict`; both DeleteObject and DeleteObjects pin that
  branch, with the latter requiring any conflict to be a per-entry error while
  its matched canary entry succeeds. A successful delete publishes the null marker
  and a successful later replacement publishes the null live version.
  Retained numbered versions, exact marker/version IDs, latest flags, metadata,
  and bytes are asserted for every branch.

  Racing an ETag-conditional delete with an ordinary marker insertion either
  publishes the conditional marker, reports `NoSuchKey` after observing the
  marker, or reports the concurrent-generation conflict. Two successful calls
  may identify one idempotently shared marker or two retained markers, so the
  matrix compares the exact unique returned IDs and requires exactly one latest
  marker. DeleteObjects includes a separately matched canary entry in every
  raced request, pins errors per key inside the successful batch response, and
  has a settled current-marker oracle requiring per-entry `NoSuchKey`. When a
  different-ETag versioned race successfully publishes a conditional marker,
  both endpoint tests require that marker to be non-latest and the later
  replacement version to be latest; accepting merely opposite latest flags
  would hide stale marker publication.

  Local metadata-command contention that escapes the bounded storage retry
  budget during an ETag-conditional delete is rendered as
  `ConditionalRequestConflict`, matching the AWS overlap response rather than
  leaking the generic `OperationAborted` used by unconditional operations.
  Route/admission failures remain `OperationAborted`, and the shared race still
  observes the first conditional response without retrying it.

  Deterministic coordinator regressions pause conceptually between entry
  authorization and the atomic storage callback. They prove that DeleteObject
  and DeleteObjects re-read a different-ETag replacement, accept and delete a
  same-ETag replacement, reject a newly current delete marker without adding
  another marker, and preserve a replaced suspended null plus its numbered
  history. No implementation change was required: the existing callback
  already keeps current authorization and condition evaluation coupled to the
  mutation.
- [x] Add the remaining conditional input and precedence matrix for operations
  that accept ETag lists or dates: weak and unquoted tags, wildcard/list forms,
  duplicate headers, malformed dates, missing objects, authorization failures,
  and GET/HEAD combined-condition ordering. Only retain parser behavior after
  it has been observed on AWS.

  The GET/HEAD slice is now oracle-pinned. AWS treats matching weak and
  unquoted ETags as matches, accepts comma lists, and combines repeated
  `If-Match` or `If-None-Match` field lines into one list regardless of field
  order. Empty `If-Match` returns 412, while empty `If-None-Match` and malformed
  conditional dates are ignored. Missing keys return `NoSuchKey` before
  condition evaluation for an authorized principal; an unauthorized principal
  receives `AccessDenied` for both existing and missing keys regardless of
  matching ETag conditions. GET and HEAD share the same status and precedence
  matrix, including ETag conditions suppressing their paired date conditions
  and `If-Match` failure taking priority over `If-None-Match`.

  CopyObject source conditions follow the same grammar: weak/unquoted matches,
  comma lists, repeated field-line combination, empty values, and malformed
  dates select the analogous success or 412 branch. Every successful case
  copies the exact source bytes, while every rejection leaves its unique
  destination absent.

  DeleteObject deliberately differs. Its `If-Match` accepts one unquoted or
  quoted ETag or the raw `*` wildcard. A weak tag and a comma list both fail
  with 412 even when they contain the current ETag; repeated `If-Match` field
  lines return `400 InvalidRequest`, and an empty value returns
  `400 InvalidArgument` with `ArgumentName` `If-Match`. Every rejected case
  retains the exact object bytes and every 204 case removes the object.

  Local now combines repeated read and copy-source condition headers and
  implements AWS's weak ETag equivalence only in GET/HEAD and CopyObject source
  evaluation. DeleteObject uses its separately pinned single-value grammar;
  DeleteObjects XML `<ETag>` remains an exact object-identifier field rather
  than HTTP conditional-header syntax.
- [x] Bound streamed PutObject/CopyObject stale-finalization retries. Direct PUT
  already has a retry/deadline budget, while the streamed finalizer currently
  loops on `StaleStreamFinalizeSnapshot`. After the public contention oracle is
  known, return the appropriate retryable S3 error on exhaustion and add a
  deterministic regression proving no publication, preserved staged cleanup,
  and no `InternalError` or indefinitely held request/reservation.
  Stream finalization now uses a one-second stale-snapshot work budget with
  contention backoff and maps exhaustion to `OperationAborted`, matching the
  established retryable public conflict branch. A deterministic intervening
  destination write expires the budget after producing a real stale snapshot:
  streamed PutObject preserves the competing object and its staged session for
  caller cleanup, while CopyObject preserves its source and competing
  destination and removes its internally owned destination stream.

## Completion

- Every added behavior was first observed on AWS and is asserted by endpoint-
  independent `s3-tests` code.
- Negative matrices contain a positive fixture/authorization canary and mutation
  failures assert final visible state.
- Targeted AWS suites and matching local suites pass.
- `cargo nextest run` and
  `cargo clippy --all-targets --all-features -- -D warnings` pass.
