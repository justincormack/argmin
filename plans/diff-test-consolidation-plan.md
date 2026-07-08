# Diff-Test Consolidation Plan

## Scope

Fold the response-shape pinning currently provided by the standalone
`crates/s3-diff-tests` crate into the main `crates/s3-tests` suite, then
expand shape coverage there and delete the separate differential crate,
`./scripts/diff-tests`, and its documentation.

## Why this makes sense

The two suites pin behaviour in different ways:

- `s3-tests` (~1,600 tests) asserts *explicit expectations* (status, error
  code, selected fields) against one endpoint at a time: the embedded local
  server on every `cargo nextest run`, or AWS via `./scripts/aws-tests`.
  AWS runs are the oracle that validates the expectations; local runs then
  enforce them continuously.
- `s3-diff-tests` (~64 tests in `response_shape.rs`, 4 scenario matrices in
  `bucket_policy.rs`) sends the same request to AWS and the embedded server
  in one process and asserts the raw responses are equal after
  normalisation (full header-set equality, exact or normalised XML bodies,
  request-id/host-id shape invariants).

The differential harness was valuable when local behaviour was far from AWS
and we could not write exact expectations. Now that conformance is close,
an *explicit* full-shape assertion in `s3-tests` pins exactly as much as a
differential comparison — provided the assertion covers the complete header
set and body, not just an error code — and it has two structural advantages:

- shape regressions are caught on every local run, whereas diff-tests only
  ran when someone invoked them with full AWS credentials
- one suite, one helper library, no dual coverage to keep in sync

What we give up: the diff harness detects AWS drift in dimensions nobody
thought to pin, automatically. We keep equivalent protection only if the new
assertions are as strict as the differential comparison: **full normalized
header-set equality and full body templates, not spot checks**. That
strictness requirement is the core design constraint below.

The `bucket_policy.rs` diff tests are already golden-expectation style
(`remote_expected: Allow/Reject` per scenario, checked against AWS, the
local server, and the in-process `auth::bucket_policy` evaluator), so they
migrate mechanically; only `response_shape.rs` needs the new helper design.

## Design: shape-assertion helpers in `s3-tests`

New module `crates/s3-tests/src/shape.rs`, exported from the crate root so
all test binaries can use it. Ported/adapted from
`crates/s3-diff-tests/tests/response_shape.rs`:

1. **Normalisation layer** (direct port):
   - lowercase header names; drop transport headers (`connection`, `date`,
     `server`, plus per-assertion extras such as `transfer-encoding`)
   - shape-validated placeholders for variable values:
     `x-amz-request-id` → `<request-id>` (16 chars, digits/uppercase),
     `x-amz-id-2` → `<host-id>` (≥40 chars base64 alphabet),
     `last-modified` → `<http-date>` (validated RFC 1123 shape),
     version IDs / upload IDs → shape-validated placeholders
   - `etag` is an explicit divergence (Argmin ETags are opaque integrity
     tokens, not AWS single-part MD5s — see `guides/aws-compatibility.md`),
     so templates treat it as a shape-checked placeholder, never a literal;
     the useful pinning is cross-response consistency (the same object's
     ETag must agree across PUT, HEAD, GET, and listing responses). This
     matches diff-tests, which ignored ETag values in comparisons.

2. **Expectation builder** — the ergonomic core. Something like:

   ```rust
   expect_shape("GetObject NoSuchKey", &resp)
       .status(404)
       .headers(exact_error_headers([...]))     // FULL set equality
       .body_template(concat!(
           r#"<?xml version="1.0" encoding="UTF-8"?>"#, "\n",
           "<Error><Code>NoSuchKey</Code>",
           "<Message>The specified key does not exist.</Message>",
           "<Key>{key}</Key>",
           "<RequestId>{request_id}</RequestId>",
           "<HostId>{host_id}</HostId></Error>",
       ))
       .assert();
   ```

   - the header expectation is complete-set equality after normalisation
     (the diff-tests property), not contains-checks
   - the body template matcher treats `{name}` as a capture with an
     attached shape validator; named captures can be cross-checked, e.g.
     `{request_id}` in the XML must equal the `x-amz-request-id` header
     (subsumes the existing `wire_ids.rs` invariants) and `{bucket}`,
     `{key}`, `{account_id}`, `{owner_id}`, `{region}` are substituted from
     the test context so one template works against AWS in any region and
     against the local server
   - a variant for repeated unordered XML blocks (the DeleteObjects
     `Deleted`/`Error` sorted-block comparison) and for presence-only tags
     (`CreationDate`, etc.)

   The full-literal `body_template` form above is the underlying
   mechanism; in practice error tests use the generated expectations from
   item 3 and rarely spell out the XML.

3. **Error expectations reuse the production error builders.** Full error
   XML literals in every test would be large and hurt readability, and
   would re-duplicate details the server already encodes. The error bodies
   are produced by a small family of builder functions in
   `crates/server-http/src/http/xml.rs` (`error_xml_with_host_id`,
   `no_such_key_error_xml`, `error_xml_with_region`,
   `metadata_too_large_error_xml`, …), and `s3-tests` already depends on
   the server stack for `TestServer`. So the shape helper offers error
   constructors that delegate to those builders (exposed `pub` as needed),
   passing the matcher's `{request_id}`/`{host_id}` capture tokens as the
   id arguments to get a template directly:

   ```rust
   expect_shape("GetObject NoSuchKey", &resp)
       .status(404)
       .headers(...)
       .body(expected_error::no_such_key("missing-key.txt"))
       .assert();
   ```

   **Circularity caveat**: against the local server, "response body ==
   builder output" is a tautology — the body assertion only bites on AWS
   runs (headers, status, and id/header-consistency checks still bite
   locally). Mitigations:
   - keep a small literal anchor set: one test per `xml.rs` builder
     function asserting the full literal XML (these stay as explicit
     templates, validated against AWS), so a builder change breaks a
     local test immediately rather than only at the next AWS run
   - any change to the error builders requires an AWS `s3-tests` run for
     the affected binaries before it lands

   Success-path bodies do not get this treatment — they are fewer, more
   varied, and the explicit template is the documentation.

4. **Matcher unit tests.** The template matcher itself gets deterministic
   unit tests in `s3-tests/src` (mismatch reporting quality matters: on
   failure print expected template, actual body, and first divergence).

Expected literals come from AWS: transcribe from a final diff-tests run
and/or validate every converted batch with `./scripts/aws-tests`. Per
AGENTS.md, AWS is the source of truth; if a template fails on AWS, the
template is wrong (or AWS changed — investigate before editing).

Known divergences documented in `guides/aws-compatibility.md` (e.g. the
`500 InternalError` cases Argmin deliberately rejects as 400) keep their
existing accept-either treatment: the test accepts both the documented AWS
shape and the Argmin shape, with a comment referencing the guide. s3-tests
must never branch on which endpoint they are running against — that rule
keeps the two behaviours from silently diverging and keeps the suite usable
as a conformance suite for other S3 implementations. The shape helpers
therefore need an "any of these shapes" form (e.g. `expect_shape_one_of`)
for these few cases, not an endpoint switch.

## Phases

### Phase 1 — helpers (DONE)

- add `crates/s3-tests/src/shape.rs` with normalisation, id-shape checks,
  expectation builder, body-template matcher, unordered-block comparator
- unit-test the matcher
- deduplicate: `wire_ids.rs` and diff-tests both carry private copies of
  `xml_tag_text` / `response_header_value` / id-shape predicates; the new
  module becomes the single home

### Phase 2 — pilot batch (DONE)

Convert a small representative slice to prove ergonomics before mass
conversion:

- error shapes: `NoSuchKey`, `NoSuchBucket`, SigV4 wrong-region and
  invalid-token (→ `object_crud.rs` / `bucket_list.rs` / auth-related
  files where the behaviour is already tested with weaker assertions)
- one success shape: PUT/GET/HEAD object round trip
- run the touched binaries locally and via `./scripts/aws-tests`; iterate
  on the helper API, then freeze it

Rule for all conversions: when an existing s3-test already covers the
behaviour, **upgrade its assertions in place** rather than adding a
parallel test — the point is to remove dual coverage, not relocate it.

Pilot outcome:

- converted: `test_object_read_not_exist` (object_crud),
  `test_list_objects_v2_no_such_bucket_error_shape` (bucket_list, new),
  `test_put_wrong_region` and
  `test_unexpected_security_token_on_static_credentials_returns_bad_request`
  (headers), `test_put_get_head_object_response_shape` (object_crud, new);
  all validated against AWS via targeted `./scripts/aws-tests` runs
- helper change from the pilot: `assert_shape` returns its placeholder
  captures (and `assert_shape_one_of` the matched index plus captures) so
  tests can pin cross-response consistency, e.g. the same `{etag}` across
  PUT/GET/HEAD of one object
- the pilot's AWS run found a modelling error in `x-amz-bucket-region` on
  the signed-header `InvalidToken` error. The full picture (established by
  probing AWS directly after a diff-test run contradicted the pilot's
  first conclusion): AWS sends the header only for bucket-scoped requests
  to existing buckets; object-scoped requests and unknown buckets omit
  it. The original server code sent it whenever the request named a
  bucket; the pilot briefly removed it entirely; the final model gates on
  bucket scope plus bucket existence (mirroring the WrongRegion gating),
  pinned by golden tests for both scopes. Lesson recorded: one AWS data
  point is one request shape — probe adjacent shapes (bucket vs object
  scope, existing vs missing resource) before concluding drift.
- a diff-test run also exposed a latent part-request bug: HEAD/GET
  `?partNumber` leaked the object-level `x-amz-checksum-type` (e.g. the
  stored default `FULL_OBJECT`) with no part checksum alongside it; AWS
  only returns the type together with a part checksum. Fixed in both part
  response builders.
- deterministic response values are pinned literally where the diff tests
  compared them: `x-amz-checksum-crc64nvme` of a fixed body,
  `x-amz-checksum-type: FULL_OBJECT`, `x-amz-server-side-encryption:
  AES256`, `content-length`

### Phase 3 — convert `response_shape.rs` in themed batches

Map each of the ~64 tests to its natural s3-tests file and convert batch by
batch, deleting each diff test once its shape coverage is subsumed. Batches
(roughly by fixture type, so AWS validation runs stay cheap):

1. bucket subresource GETs (location, versioning, encryption, CORS,
   tagging, lifecycle, public-access-block, ownership, policy-status, ACL)
   — DONE: ten golden tests added to the matching s3-tests files, all
   AWS-validated first try, ten diff tests deleted. Notes: several
   subresource GETs carry no `Content-Type` (versioning, encryption,
   CORS, tagging, public-access-block); lifecycle and ownership use
   `Content-Length` instead of chunked encoding; AWS adds
   `x-amz-transition-default-minimum-object-size` on lifecycle but Argmin
   deliberately omits it while transitions are unimplemented (documented
   in aws-compatibility.md, test accepts either — first real use of
   `assert_shape_one_of`); GetBucketAcl has no `DisplayName` (AWS
   deprecated it) and pins `{owner_id}` consistency between Owner and
   Grantee; policy-status without a policy is a `NoSuchBucketPolicy`
   404. Shape helpers gained the header-set vocabulary `id_headers` /
   `chunked_response_headers` / `xml_response_headers`.
2. object CRUD, range/override, checksum-mode, website-redirect and
   metadata/header-limit errors — DONE: thirteen diff tests converted
   (13 golden tests across website_redirect, object_crud, checksums,
   headers, range, bucket_anon, and versioning), all AWS-validated
   first try, thirteen diff tests deleted. In-place upgrades:
   metadata/system-metadata/header-section limit tests in object_crud
   (now pin exact Size/MaxSizeAllowed), the invalid-redirect rejection
   in website_redirect, and the anonymous AccessDenied test in
   bucket_anon. New helpers: `raw_anonymous` raw request and
   `expected_error::request_header_section_too_large`. Versioned-object
   tests pin `{version_id}` from the SDK PUT across GET/HEAD
   current/explicit, and the delete-marker version from the DELETE
   response captures.
3. SSE-C success and error shapes — DONE: six diff tests converted
   (five golden tests in sse_c.rs, one default-encryption body test in
   bucket_encryption.rs), all AWS-validated first try, six diff tests
   deleted. The blocked-by-default AccessDenied message covers the
   endpoint-varying caller principal with `{any}`; the SSE-C
   InvalidArgument bodies are inline formats in response.rs (not xml.rs
   builders) so those tests use literal templates via a local helper.
4. multipart success shapes and completion/part errors, upload-part-copy
   — DONE: eleven diff tests converted to golden tests in multipart.rs,
   all AWS-validated, eleven diff tests deleted. The old diff test only
   compared HEADERS for the create/upload/list-parts/complete flow, and
   probing AWS for full bodies exposed four server conformance bugs, all
   fixed: InitiateMultipartUploadResult carried ChecksumAlgorithm/Type
   elements AWS keeps header-only; ListPartsResult lacked
   Initiator/Owner/StorageClass/ChecksumType/NextPartNumberMarker and
   used a different element order; CompleteMultipartUpload Location was
   a hardcoded http://s3.amazonaws.com host (now the regional
   https://s3.{region}.amazonaws.com path-style form AWS returns); and
   the complete-body ETag was entity-escaped where AWS emits raw quotes
   (AWS is inconsistent: ListParts entries ARE entity-escaped).
   Multipart completion error bodies carry no XML declaration —
   expected_error gained eight wrappers with literal anchors. Shape
   templates gained the possibly-empty `{ws}` placeholder for AWS's
   keep-alive whitespace padding in slow CompleteMultipartUpload
   responses. Note: the golden flow test pins full bodies, strictly more
   than the old diff test checked.
5. object lock (bucket config, retention, legal hold) — DONE: four
   golden tests added to object_lock.rs reusing its existing fixtures
   and cleanup helpers (no shared-helper move needed), all
   AWS-validated first try, four diff tests deleted plus the five
   pilot-batch diff twins that were never removed. Lock timestamps
   appear without milliseconds in headers and with `.000Z` in XML
   bodies, both derived in-test from the retention epoch. Batch note:
   an early probe bug (all scenarios in one bucket) tripped the real
   governance-shortening denial — retention scenarios need buckets
   without a default-retention config. Follow-up from that: a new
   golden test pins shortening default-derived retention (the one
   variant existing shorten tests missed), and probing AWS for its
   error shape found the denial message divergence — AWS returns
   "Access Denied because object protected by object lock." for the
   whole lock-protection family (default and explicit shorten, and
   delete without bypass, all probed). Added
   ServerError::ObjectLockProtectedAccessDenied carrying that message
   for retention-update and delete lock denials; policy/bypass
   permission denials stay generic AccessDenied — enforced in the
   validators by distinguishing bypass-not-requested (lock message)
   from bypass-requested-but-denied, which AWS-probing showed returns
   an IAM-style s3:BypassGovernanceRetention denial message; Argmin
   returns generic Access Denied there for now (documented under the
   principal-specific-message gap in aws-compatibility.md §13). The
   explicit-shorten and delete-without-bypass golden shapes are pinned
   in upgraded object_lock tests, AWS-validated. Review follow-up: the
   locked-read golden test was extended to restore the deleted diff
   test's full surface — explicit-version GET/HEAD, plain-object HEAD,
   and GetObjectAttributes on a locked object (which carries no lock
   headers and no content-type; absence pinned via full-set equality).
6. listings (v1/v2, versions, multipart uploads) and DeleteObjects —
   DONE: five golden tests (object_delete, bucket_list ×2, versioning,
   multipart), all AWS-validated first try, five diff tests deleted.
   DeleteObjects uses the canonicalizing unordered-block helper
   (`assert_body_with_unordered_blocks`, added on review feedback):
   matched Deleted blocks are stripped and the remainder must equal the
   envelope exactly, so extra sibling elements cannot slip through. Full
   listing bodies are pinned with `{version_id}`/upload-id subs from
   fixture responses; the v1 no-NextMarker property is enforced by
   full-template equality rather than a separate absence check.
   Initiator IDs stay `{any}` (ARN on AWS, canonical ID locally);
   ListObjects v1/v2 carry `x-amz-bucket-region`, the other listings
   do not.
7. POST object, CopyObject, GetObjectAttributes, delete-marker shapes
   — DONE (delete-marker shapes were converted in batch 2): six golden
   tests across bucket_crud, tagging, object_crud, post_object,
   copy_object, and object_attributes, all AWS-validated first try.
   HeadBucket is the second accept-either case (AWS sends
   Transfer-Encoding: chunked on HEAD, Argmin does not — guide §9).
   Object tag order is not pinned on either endpoint, so the tagging
   body uses the canonicalizing unordered-block helper. The PostObject
   Location authority is endpoint-specific; the pattern bakes the
   bucket/key into the template so `{any}` covers only the authority.
   With this batch response_shape.rs is EMPTY and has been deleted
   (Cargo.toml test target removed; guide/script examples now point at
   bucket_policy) — only the Phase 4 bucket-policy matrices remain in
   the diff crate.

Each batch ends with: local `cargo nextest run` for the touched binaries,
`./scripts/aws-tests --test <binary>` green, corresponding diff tests
deleted.

### Phase 4 — migrate the bucket-policy matrices (DONE)

- move the scenario runner and the four matrices from
  `s3-diff-tests/tests/bucket_policy.rs` into the s3-tests bucket-policy
  files; the AWS-vs-local dual execution collapses to the normal CTX
  single-endpoint model with `remote_expected` as the golden outcome
- the in-process `auth::bucket_policy` evaluator cross-check is unit-level
  coverage; move it into the `auth` crate's own tests keyed by the same
  scenario table, or drop it if the crate already covers those conditions

Outcome: the matrices moved wholesale to
`crates/s3-tests/tests/bucket_policy_conditions.rs` with the scenario
tables verbatim; DiffEnv collapsed to CTX owner/alt clients, and the
alt-account principal comes from `CTX.alt_account_id()` in both modes.
Rather than duplicating the large scenario table into the auth crate,
the evaluator cross-check stayed inline as a pure model check — it is
endpoint-independent (no branching) and keeps a single source of truth
for the scenarios. All four matrices pass locally and against AWS
(the ACL/grant matrix takes ~65s on AWS with policy-convergence
retries). The diff crate now has zero tests. Review follow-up folded
in: `scripts/diff-tests` is deleted and all guide references
(testing.md quick-reference, script list, differential section,
crate-choice guidance, aws-compatibility §9 note, security-testing
matrix) now point at the golden shape assertions and
`bucket_policy_conditions`, so no advertised command is broken; only
the empty crate itself (and its scripts/ci compile step) remains for
Phase 5. Second review follow-up: the scenario table contained two
never-selected ExistingTagRead(ObjectAttributes) scenarios (dead in
the original diff suite too); wiring them in exposed that their
expectations had never been validated — AWS rejects
GetObjectAttributes even when the existing-tag condition matches
(ExistingObjectTag is not evaluable for that action), which is
exactly how the auth evaluator already models it. The scenario now
pins Reject/NoMatch, AWS-validated, and the tagging matrix runs all
seven of its shapes.

### Phase 5 — removal (DONE)

As soon as all diff tests are ported (end of Phase 4), remove the old
crate — expansion does not need it around:

- delete `crates/s3-diff-tests` and `./scripts/diff-tests`
- update `guides/testing.md` (remove the differential section, document the
  shape-assertion convention and helper module instead), `AGENTS.md` if it
  references diff-tests, and any CI references
- full `./scripts/aws-tests` run to confirm nothing was lost in porting

Outcome: `scripts/diff-tests` and the guide updates landed with Phase 4;
this phase deleted the empty crate, its `scripts/ci` compile step, and
the workspace `exclude`, and closed with the full AWS `s3-tests` run:
73 of 74 binaries green. The single failure was pre-existing drift
unrelated to the porting, in
`test_bucket_policy_foreign_owned_object_access_matrix_matches_aws`:
AWS no longer lets a bucket policy grant the bucket owner PutObjectAcl
on a foreign-owned object ("no resource-based policy allows the
s3:PutObjectAcl action"), closing the old anomaly where ACL reads were
denied but ACL writes grantable; tagging operations remain grantable
(full matrix re-probed). FOLLOW-UP SLICE (DONE 2026-07-07): the foreign-owned
bucket-policy filter now covers both ACL directions
(`filter_bucket_policy_allow_for_foreign_owned_object_action`, applied
in all object-action policy decision paths, no-op for unlisted
actions), and the pinned ownership matrix expects the denial; the full
ownership binary passes on AWS. Context: AWS has been making denial messages more
explicit over recent weeks (the probed "no resource-based policy
allows"/"explicit deny in a resource-based policy" variants belong to
that wave), so message-level drift is expected elsewhere too — a
periodic full `./scripts/aws-tests` run is the detector.

### Phase 6 — expand shape coverage (the actual goal, open-ended)

With helpers frozen and the old crate gone, adopt a convention for new and
existing tests:

- error-path tests assert the full error shape via `expect_shape`, not
  just the code
- success-path tests for operations with a stable body assert the full
  template
- sweep the highest-value gaps first: operations that have raw-request
  coverage but only status/code assertions today (`grep` for
  `assert_s3_err_code` / `err_status` call sites is the worklist —
  currently ~770 call sites; convert opportunistically, prioritising
  responses users parse: listings, multipart, versioning, ACL/policy
  errors)

Priority list (surveyed 2026-07-07, weakest pinning first — these had no
diff-test coverage either, so their shapes have never been AWS-pinned):

1. **ListBuckets (`GET /`)** — no shape test at all; clients parse the
   Owner/DisplayName/Buckets body, and it is the one listing the batch-6
   work did not cover.
   **DONE (slice 1, 2026-07-07)** — AWS probing found four divergences,
   all fixed server-side: Owner carries no `DisplayName`; each Bucket
   gains `BucketRegion` + `BucketArn`; a `prefix` query param filters
   and is echoed as `<Prefix>` after `</Buckets>` (omitted entirely when
   the param is absent — probed both ways); body is chunked, not
   content-length. `test_list_buckets_response_shape` (bucket_crud.rs)
   pins the prefix-filtered shape; AWS-validated. Not covered:
   `max-buckets`/`continuation-token` pagination is still ignored
   locally (needs a many-bucket AWS probe to pin the truncation shape).
2. **Conditional request shapes** — `conditional.rs` asserts 304/412 only
   via SDK `is_err`; 304 has a distinctive no-body/no-content-type shape
   and 412 `PreconditionFailed` bodies are pinned only for the
   upload-part-copy variant.
   **DONE (slice 1, 2026-07-07)** — plain-object 412 on AWS carries the
   canonical message plus a `<Condition>` element naming the failing
   header and a `HostId` (local had lowercase "precondition failed",
   empty `<Resource>`, no HostId). `ServerError::PreconditionFailed` now
   carries the condition name from every construction site (read, write,
   delete, copy-source); the POST-Object wrong-content-type 412 was
   probed separately (Condition is the sentence "Bucket POST must be of
   the enclosure-type multipart/form-data") and its existing test
   upgraded to a full golden. `test_conditional_response_shapes` pins
   304 / GET-412 / PUT-If-None-Match-star-412; AWS-validated. The
   UploadPartCopy 412 keeps its own builder: AWS omits the XML
   declaration there but includes it on plain-object 412s.
3. **Plain GET `416 InvalidRange`** — `range.rs` has no 416 coverage at
   all despite the builder (`invalid_range_error_xml`) and its
   `expected_error` wrapper already existing.
   **DONE (slice 1, 2026-07-07)** — AWS adds `<RangeRequested>` between
   Message and ActualObjectSize; builder and
   `ServerError::InvalidRange` extended to carry the requested range.
   `test_get_object_invalid_range_error_shape` pins it; AWS-validated.
4. **Bucket policy CRUD** — `bucket_policy.rs` has ~187 weak assert
   sites; GetBucketPolicy's JSON echo body and the MalformedPolicy /
   policy-validation error family are unpinned.
   **DONE (slice 2, 2026-07-08)** — CRUD raw shapes pinned
   (`test_bucket_policy_crud_response_shapes`: Put/Delete 204 acks,
   Delete idempotent, Get 200 `application/json` exact echo; template
   grammar gained `{{`/`}}` escapes + `escape_literal` for JSON bodies).
   AWS probing found the local MalformedPolicy family diverged: wrong
   messages (`invalid JSON` vs "Policies must be valid JSON and the
   first byte must be '{'", `missing Statement` vs "Missing required
   field Statement"), Resource-style bodies instead of HostId+`<Detail>`,
   and three validation gaps — empty `Statement` arrays, resources not
   scoped to the bucket (`arn:aws:s3:::*` included), and malformed IAM
   principal ARNs were all accepted. All fixed: `BucketPolicyError` and
   `ServerError::MalformedPolicy` carry an optional `Detail`; empty
   statements are rejected by the parser and resource scoping at
   PutBucketPolicy time (the parser has no bucket context); applicability
   errors carry AWS's `Action "…" in Statement "NO_ID-{n}"` label.
   `test_put_bucket_policy_malformed_response_shapes` pins ten error
   shapes (including the string-form principal rule: AWS accepts only
   `"*"` in string position — any other string, even a well-formed root
   ARN, is `Invalid policy syntax.`); AWS-validated. Principal *existence* is not validated (see
   aws-compatibility.md §14). Remaining unpinned here: the
   MalformedPolicy oversized-normalized-policy message and the ~187
   weak semantic asserts (convert opportunistically).
5. **Public-access / anonymous error surfaces** — the `public_access_*`
   files assert codes only; these are security-relevant responses.
   **DONE (slice 3, 2026-07-08)** — AWS probing found three divergences,
   fixed server-side: `x-amz-bucket-region` now attaches to 403
   AccessDenied on the region-discovery surfaces (HeadBucket,
   ListObjects v1/v2) for existing buckets in any auth mode (probed
   anonymous and cross-account; subresource GETs and PutBucketPolicy
   denials omit it, also probed); the missing-PublicAccessBlock 404 got
   AWS's body (message "The public access block configuration was not
   found", `BucketName`, HostId); and `error_xml_with_host_id` now
   text-escapes messages so quotes render raw like AWS (was `&quot;`).
   bucket_anon.rs upgraded in place (nine weak asserts → full shapes,
   incl. the paired private-403/missing-404 contract), plus new goldens
   `test_public_access_block_crud_response_shapes` and
   `test_block_public_policy_denial_response_shape` (principal as
   `{any}` — the message names the requester, see §13/§14).
   AWS-validated: full bucket_anon binary + both new goldens.
   Remaining in this family: the authenticated cross-account denial
   bodies are AWS principal-specific messages (§13) and stay unpinned;
   public_access_acl/block_acl/object_lock/post_object/range files
   still weak — convert opportunistically.
6. **Presigned and chunked-upload error shapes** — `presigned.rs` and
   `chunked.rs` use `contains` checks on bodies; the SigV4 error family
   is only partially shaped (pilot covered two cases).
   **DONE (slice 4, 2026-07-08)** — chunked.rs turned out to already be
   strong (one `contains`); presigned.rs (33 `contains`, 0 shapes) is
   now fully shaped. AWS probing found five divergences, fixed
   server-side: SignatureDoesNotMatch now carries AWS's diagnostic echo
   (AWSAccessKeyId, StringToSign, SignatureProvided, hex byte dumps,
   CanonicalRequest) threaded from both SigV4 verifiers via
   `AuthError::SignatureMismatch { diagnostics }` (POST policy and
   chunk-signature mismatches carry none — their AWS shapes are
   unprobed); expired presigned URLs echo X-Amz-Expires/Expires/
   ServerTime (no-millis ISO); missing query auth parameters use AWS's
   fixed all-parameters sentence and over-week X-Amz-Expires its
   dedicated message, both in HostId-style bodies; HeadersNotSigned
   dropped its Resource element. The three shared assert helpers now
   pin full bodies (via new `assert_status_and_body` for ureq paths),
   and the probed cases pin headers too via `raw_fetch_url`. The whole
   41-test presigned binary is AWS-validated.
   **Follow-up (review findings, 2026-07-08)**: the POST-policy and
   chunk-signature mismatch bodies were then probed too — both carry
   diagnostics (POST echoes the policy as StringToSign with no
   CanonicalRequest; chunk mismatches echo the chunk string-to-sign
   plus the seed request's canonical request, so
   `StreamingSigningContext` now carries the access key and seed
   canonical request) — and the not-yet-valid presigned body gained
   AWS's X-Amz-Date (epoch millis)/Expires/ServerTime elements. The
   epoch-date/future-date/POST/chunk tests are full goldens now, all
   AWS-validated. **Follow-up 2 (review findings, 2026-07-08)**: the earlier "chunked
   was already strong" note was wrong — its 24 `assert_error_code`
   sites pinned codes only. A full-body probe of every chunked error
   family (running a patched copy of the binary against AWS) confirmed
   the trailer-signature diagnostic render (AWS echoes the
   AWS4-HMAC-SHA256-TRAILER string-to-sign plus the seed canonical
   request) and found five more divergences, fixed: IncompleteBody
   ("The request body terminated unexpectedly"), MissingContentLength
   ("You must provide the Content-Length HTTP header."),
   MalformedTrailerError (one fixed AWS sentence for all variants),
   InvalidChunkSizeError (AWS message plus `<Chunk>`/`<BadChunkSize>`,
   where Chunk is the 1-based detection chunk), and unsupported
   streaming tokens (dedicated `UnsupportedStreamingToken` rendering
   AWS's token-list message with ArgumentName/ArgumentValue); the
   checksum-header InvalidRequest moved to the HostId body via
   `InvalidRequestHostId`. All 24 sites now pin full bodies, and the
   expired presigned PUT pins the full expired shape. Both the 47-test
   chunked and 41-test presigned binaries are AWS-validated. Still
   unprobed: the non-numeric X-Amz-Expires message.
7. **Malformed-XML request errors** — `malformed_xml.rs` body checks are
   `contains`-based; `MalformedXML` bodies carry no declaration (like
   the multipart errors) and would anchor cheaply.
   **DONE (slice 5, 2026-07-08)** — probed every case via a printing
   copy of the binary on AWS (single-threaded for attribution). AWS
   uses ONE canonical MalformedXML message everywhere, in two
   transport variants scoped by operation: CompleteMultipartUpload,
   DeleteObjects, and OwnershipControls render without the XML
   declaration, other operations with it. Local's ~19 bespoke messages
   replaced by the canonical sentence at render time (reasons stay
   internal); new `MalformedXMLNoDecl` variant for the no-decl
   parsers. Other fixes: DeleteObjects KeyTooLongError loses the
   declaration and gains HostId (direct PUT keeps the declaration —
   both probed — and over-long PUT keys now return KeyTooLongError
   instead of InvalidRequest); InvalidTag echoes `<TagKey>`/`<TagValue>`
   (lossy text for invalid UTF-8); bad tagging-header percent-encoding
   returns AWS's InvalidArgument header sentence with
   ArgumentName/ArgumentValue; IllegalVersioningConfigurationException
   and the CORS unsupported-method InvalidRequest move to HostId
   bodies. All 30 malformed_xml tests pin full bodies plus a new
   direct-PUT KeyTooLongError golden; binary AWS-validated.
8. **Subresource write acks** — PutBucketVersioning/Tagging/etc. 200s
   and Delete* 204s: header sets never pinned (cheap, mechanical).

Remaining weak-assert density by file (top of the grep worklist):
object_lock (98), tagging (49), sse_c (45), multipart (36),
conditional (33), ownership (27), checksums (25), bucket_acl (22) —
many of these sit next to now-golden tests and can be upgraded
opportunistically when those files are touched.

This phase is open-ended; track progress in this plan as batches land.

## Risks and notes

- **Strictness regression risk**: a lazy conversion (status + code only)
  silently loses pinning the diff harness had. Review each conversion
  against the original diff test's normalisation lists — anything the diff
  test compared, the golden test must assert.
- **Region parameterisation**: diff-tests started the local server in the
  AWS region to make bodies comparable; golden templates instead take
  `{region}`-dependent values from CTX. Tests that were explicitly about
  region matching (e.g. HeadBucket when regions match) need a deliberate
  re-expression, not a mechanical port.
- **Owner/canonical IDs**: AWS canonical owner IDs and account-dependent
  values must be placeholders with shape validation plus cross-response
  consistency checks, never literals.
- **AWS drift**: golden templates freeze today's AWS responses. That is
  the existing s3-tests philosophy; `./scripts/aws-tests` runs remain the
  drift detector, so keep running the full AWS suite at the existing
  cadence.
