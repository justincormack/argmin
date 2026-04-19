# Object Redirect Metadata REST Plan

## Scope

This plan covers AWS-compatible support for object-level website redirect
metadata on the normal S3 REST endpoint only.

In scope:

- request-side support for object redirect metadata on REST operations that AWS
  exposes today:
  - `PutObject`
  - `CreateMultipartUpload`
  - `CopyObject`
  - `POST Object`
- response-side support for redirect metadata on REST operations that surface
  stored object metadata:
  - `HeadObject`
  - `GetObject`
- persistence of redirect metadata as first-class object metadata
- AWS-compatible copy and multipart semantics for that metadata
- comprehensive `crates/s3-tests` coverage, including explicit AWS conformance
  tests for behaviors that are unclear or only partially documented

Out of scope:

- S3 website endpoint behavior
- bucket website configuration APIs:
  - `PutBucketWebsite`
  - `GetBucketWebsite`
  - `DeleteBucketWebsite`
- website-endpoint redirect serving
- routing rules, index documents, or error documents
- directory bucket behavior

## Why This Needs A Dedicated Plan

This surface is small enough to implement, but the AWS documentation is not a
complete specification by itself. The behavior is scattered across multiple API
reference pages and the user guide, and some operational semantics are only
implied.

That means the implementation should not start from code changes. It should
start from AWS-backed tests that map out the behavior we actually need to
match.

The scope here is intentionally narrower than "website behavior":

- object redirect metadata on the REST endpoint is one compatibility feature
- website endpoint redirect serving is a different feature
- bucket website configuration is a separate API surface

We should not blur them together.

## AWS Behaviors To Pin Down First

The following items are either poorly documented, documented only indirectly,
or scattered enough that we should treat them as test-discovered behavior.

### Request acceptance and validation

We need AWS-backed tests for:

- accepted value forms:
  - same-bucket path starting with `/`
  - absolute `http://...`
  - absolute `https://...`
- rejected value forms:
  - missing leading `/` for same-bucket-style redirects
  - unsupported schemes
  - empty value
  - oversized value
- exact error code / status / message shape for invalid values where practical

### REST metadata visibility

We need AWS-backed tests for:

- `HeadObject` returning `x-amz-website-redirect-location`
- `GetObject` returning `x-amz-website-redirect-location`
- whether zero-byte and non-zero-byte objects behave identically
- whether versioned `GET`/`HEAD` by explicit version id surfaces the metadata
  from the selected version

### Copy semantics

We need AWS-backed tests for:

- `CopyObject` with `MetadataDirective=COPY` and no explicit redirect header:
  redirect metadata should not copy
- `CopyObject` with explicit redirect header:
  destination redirect metadata should use the new value
- `CopyObject` with `MetadataDirective=REPLACE` and explicit redirect header:
  replacement metadata plus redirect metadata should persist together
- same-key self-copy legality:
  changing redirect metadata alone should make an otherwise-illegal self-copy
  request legal
- same-key self-copy illegality:
  if redirect metadata and all other relevant attributes are unchanged, the
  request should still fail with AWS-compatible behavior

### Multipart semantics

We need AWS-backed tests for:

- `CreateMultipartUpload` accepting redirect metadata
- completed object preserving redirect metadata from multipart initiation
- `CompleteMultipartUpload` not having any way to add or replace redirect
  metadata
- multipart uploads without redirect metadata staying unset after completion

### POST object semantics

We need AWS-backed tests for:

- multipart form field acceptance for `x-amz-website-redirect-location`
- stored redirect metadata surfacing on later `HEAD`
- POST policy enforcement:
  - allowed when policy conditions include the field
  - rejected when policy conditions omit the field
  - rejected when the field value does not match an exact policy condition

## Test-First Deliverable

Phase 1 should produce a comprehensive `s3-tests` file for REST object redirect
metadata before Argmin implementation begins.

Recommended test file:

- `crates/s3-tests/tests/website_redirect.rs`

The initial file should be structured as an AWS-conformance matrix rather than
as implementation-driven tests.

### Minimum test matrix

1. `PutObject`
- set redirect metadata on upload
- `HeadObject` returns header
- `GetObject` returns header
- invalid values rejected

2. `CopyObject`
- explicit redirect metadata on destination persists
- `MetadataDirective=COPY` does not implicitly copy redirect metadata
- redirect-only self-copy succeeds
- identical self-copy still fails

3. `CreateMultipartUpload` + `UploadPart` + `CompleteMultipartUpload`
- redirect metadata set at initiation persists to final object
- absent redirect metadata remains absent

4. `POST Object`
- accepted redirect field persists
- policy-controlled redirect field acceptance and rejection

5. Versioning interaction
- redirect metadata persists on the correct version
- `HEAD` / `GET` by version id returns the version’s redirect metadata

### Test methodology

Use AWS as the oracle wherever docs are ambiguous.

- Prefer focused tests that exercise one rule at a time.
- Use raw signed requests where the SDK hides important wire details.
- Keep SDK-based positive tests where the modeled operation surface is enough.
- Record AWS-only observations in comments when behavior is surprising or not
  directly stated in docs.

If an observed AWS behavior contradicts our initial assumptions, the tests win
and the plan should be updated.

## Implementation Shape

Once the AWS matrix is in place, the implementation should follow the existing
HTTP/core/storage split.

### 1. Represent redirect metadata as system metadata

`x-amz-website-redirect-location` is not user metadata and should not be stored
inside `MetadataBlob`.

Instead, extend `SystemMetadata` in:

- [system_metadata.rs](/home/justin/src/github.com/justincormack/argmin/crates/server-core/src/system_metadata.rs)

to carry an optional redirect location field alongside the existing first-class
system metadata.

This keeps the value:

- visible to request parsing and response rendering
- separate from `x-amz-meta-*`
- subject to explicit validation and serialization rules

### 2. Parse the request header at the HTTP boundary

Extend request parsing in:

- [mod.rs](/home/justin/src/github.com/justincormack/argmin/crates/server-http/src/http/mod.rs)

so the REST write paths that AWS supports can accept
`x-amz-website-redirect-location`:

- `PutObject`
- `CreateMultipartUpload`
- `CopyObject`
- `POST Object`

Validation should stay at the HTTP boundary, with error mapping matched to AWS
based on the test matrix.

### 3. Persist and replay through coordinator/storage

The write paths should thread the parsed value through:

- `PutObject`
- streaming `PutObject`
- `CreateMultipartUpload`
- multipart completion finalization
- `CopyObject`

and the read paths should replay it through:

- `HeadObject`
- `GetObject`

The stored representation should reuse the existing serialized system metadata
blob, not introduce a separate object column unless a later constraint forces
that decision.

### 4. Copy semantics must be explicit

`CopyObject` needs dedicated logic for redirect metadata, not generic metadata
cloning.

The existing copy path already distinguishes:

- metadata directive behavior
- tagging behavior
- same-object self-copy legality

Relevant code:

- [copy.rs](/home/justin/src/github.com/justincormack/argmin/crates/server-core/src/coordinator/copy.rs)
- [request_types.rs](/home/justin/src/github.com/justincormack/argmin/crates/server-core/src/coordinator/request_types.rs)

That path should be extended so redirect metadata follows AWS’s special rule:

- it is object-specific
- it is not implicitly copied by metadata copy semantics
- explicitly changing it counts as a meaningful self-copy change

### 5. POST policy enforcement must include the field

If AWS accepts `x-amz-website-redirect-location` on `POST Object`, then POST
policy matching needs to treat it like other signed policy-controlled form
fields.

That work belongs with the existing POST object policy enforcement, not as a
special case hidden in the storage layer.

## Implementation Phases

### Phase 1: AWS Conformance Tests

Deliverables:

- add `crates/s3-tests/tests/website_redirect.rs`
- cover the request, response, copy, multipart, POST, and versioning cases
  listed above
- annotate any AWS-discovered behavior that is absent or ambiguous in docs

Exit criteria:

- we have a stable AWS-backed behavior matrix for REST object redirect metadata
- the remaining unknowns are minimal and explicitly documented in the test file

### Phase 2: Typed Metadata Plumbing

Deliverables:

- extend `SystemMetadata`
- extend serialization/deserialization for stored system metadata
- add unit tests for metadata round-tripping

Exit criteria:

- redirect metadata can be stored and loaded as first-class system metadata

### Phase 3: Write-Path Support

Deliverables:

- support request parsing and validation on:
  - `PutObject`
  - `CreateMultipartUpload`
  - `CopyObject`
  - `POST Object`
- thread the value through coordinator request types and write paths

Exit criteria:

- all supported REST write paths can persist redirect metadata

### Phase 4: Read-Path Support

Deliverables:

- emit `x-amz-website-redirect-location` on:
  - `HeadObject`
  - `GetObject`
- verify versioned and nonversioned behavior against the AWS tests

Exit criteria:

- REST metadata visibility matches AWS for the tested matrix

### Phase 5: Copy And Multipart Edge Cases

Deliverables:

- implement the tested `CopyObject` special cases
- implement multipart initiation-to-completion persistence semantics
- ensure self-copy legality checks match AWS

Exit criteria:

- all copy and multipart redirect tests pass locally and against AWS

## Verification

Minimum verification for this work:

- focused `cargo test -p s3-tests --test website_redirect`
- adjacent regression tests for:
  - `copy_object`
  - `multipart`
  - `post_object`
  - `headers`
  - `versioning`
- `cargo test -p server-core --lib`
- `cargo test -p server-http --lib`
- `cargo clippy --all-targets --all-features -- -D warnings`

Before any final commit for the implementation work, run the full test suite as
required by the repo guidance.
