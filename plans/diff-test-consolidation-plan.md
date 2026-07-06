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
   metadata/header-limit errors
3. SSE-C success and error shapes
4. multipart success shapes and completion/part errors, upload-part-copy
5. object lock (bucket config, retention, legal hold) — needs the
   object-lock cleanup helper moved into shared helpers
6. listings (v1/v2, versions, multipart uploads) and DeleteObjects
7. POST object, CopyObject, GetObjectAttributes, delete-marker shapes

Each batch ends with: local `cargo nextest run` for the touched binaries,
`./scripts/aws-tests --test <binary>` green, corresponding diff tests
deleted.

### Phase 4 — migrate the bucket-policy matrices

- move the scenario runner and the four matrices from
  `s3-diff-tests/tests/bucket_policy.rs` into the s3-tests bucket-policy
  files; the AWS-vs-local dual execution collapses to the normal CTX
  single-endpoint model with `remote_expected` as the golden outcome
- the in-process `auth::bucket_policy` evaluator cross-check is unit-level
  coverage; move it into the `auth` crate's own tests keyed by the same
  scenario table, or drop it if the crate already covers those conditions

### Phase 5 — removal

As soon as all diff tests are ported (end of Phase 4), remove the old
crate — expansion does not need it around:

- delete `crates/s3-diff-tests` and `./scripts/diff-tests`
- update `guides/testing.md` (remove the differential section, document the
  shape-assertion convention and helper module instead), `AGENTS.md` if it
  references diff-tests, and any CI references
- full `./scripts/aws-tests` run to confirm nothing was lost in porting

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
