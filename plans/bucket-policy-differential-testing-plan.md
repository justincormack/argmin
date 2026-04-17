# Bucket Policy Differential Testing Plan

## Scope

This plan adds generated and differential test coverage for
`BucketPolicy::evaluate` in `crates/auth/src/bucket_policy.rs`.

In scope:
- randomized generation over supported bucket-policy statements and
  `PolicyRequest` shapes
- local metamorphic and differential checks for evaluator behavior
- an AWS-backed oracle for the supported condition subset that can be
  materialized through real S3 requests
- promotion of mismatches into fixed regressions and fuzz corpus seeds

Out of scope:
- generic request-parser fuzzing, which is already tracked in
  `plans/parser-hardening-plan.md`
- unsupported bucket-policy condition keys or IAM language expansion
- replacing the coordinator-level authz model in
  `plans/authz-model-testing-plan.md`
- trying to prove all final authz semantics through `BucketPolicy::evaluate`
  alone

## Motivation

`crates/auth/src/bucket_policy.rs` already has a large number of unit tests,
but they are still example-driven.

That is no longer enough for this surface:

- the evaluator combines principal matching, action/resource wildcards,
  deny-precedence, and a growing set of condition keys
- the currently fuzzed auth coverage stops at request parsing and does not
  exercise evaluator semantics
- the coordinator now depends on a supported subset of bucket-policy condition
  keys for real authorization decisions

The next failure here is more likely to be semantic drift than a parser panic.

## Current State

- `BucketPolicy::evaluate` is covered by many focused unit tests in
  `crates/auth/src/bucket_policy.rs`
- `validate_evaluable_object_conditions` and
  `condition_clause_supported_for_evaluable_object_actions` define the object
  condition subset the coordinator relies on today
- `fuzz/Cargo.toml` has parser/front-door targets but no bucket-policy
  evaluator target
- `crates/s3-tests/tests/bucket_policy.rs` provides important AWS-backed
  scenario coverage, but it is hand-authored rather than generated

## Goals

- find evaluator regressions with generated scenarios instead of relying only on
  handwritten examples
- compare Argmin bucket-policy decisions with AWS for the subset of policies and
  request context we intentionally support
- make failures reproducible by rendering the exact policy JSON and request
  shape that triggered the mismatch
- keep unsupported condition keys explicit and out of the generator, so
  failures stay high signal

## Non-Goals

- do not introduce a generic policy engine or IAM dependency
- do not try to support every AWS bucket-policy condition key as part of this
  plan
- do not make live AWS differential runs a required presubmit gate if they are
  too slow or too flaky; they can start as targeted or periodic coverage
- do not infer unsupported semantics from AWS and silently widen local behavior

## Design Constraints

### 1. Generate Only the Supported and Materializable Subset

The first generator should stay inside the subset we both evaluate locally and
can turn into real AWS requests.

Start with:

- actions already represented by `PolicyAction`
- principals that are easy to materialize in tests:
  - `*`
  - owner account root or user
  - alternate account root or user
  - alternate account canonical user where the AWS SDK path can observe it
- resource shapes that map cleanly to bucket or object scope
- condition keys/operators already accepted and evaluated for object actions:
  - `s3:ExistingObjectTag/*` with `StringEquals` and `StringEqualsIfExists`
  - `s3:RequestObjectTag/*`
  - `s3:x-amz-copy-source`
  - `s3:x-amz-metadata-directive`
  - `s3:x-amz-acl`
  - `s3:x-amz-server-side-encryption`
  - `s3:x-amz-server-side-encryption-customer-algorithm`
  - `s3:x-amz-grant-read`
  - `s3:x-amz-grant-write`
  - `s3:x-amz-grant-read-acp`
  - `s3:x-amz-grant-write-acp`
  - `s3:x-amz-grant-full-control`

Do not start with unsupported or operationally awkward keys such as:

- VPC/VPCE conditions
- source IP conditions
- time-based conditions
- conditions that are accepted by the parser but intentionally not part of the
  currently enforced object subset

### 2. Keep Evaluator Semantics Separate from Final Authz Semantics

AWS exposes the outcome of the whole S3 authorization pipeline, not
`BucketPolicy::evaluate` directly.

The differential harness therefore has to isolate bucket policy as the
decisive variable:

- use requesters that do not receive accidental access through object ACLs
- control bucket ownership mode and public-access-block settings explicitly
- choose request shapes where success or `AccessDenied` can be attributed to
  the generated bucket policy rather than some unrelated grant

This also means the AWS-backed harness should compare observable classes, not
raw implementation internals.

### 3. Distinguish `Allow`, `Deny`, and `NoMatch` Deliberately

AWS gives a visible allow/deny answer, but `BucketPolicy::evaluate` has three
outcomes:

- `ExplicitAllow`
- `ExplicitDeny`
- `NoMatch`

To separate `ExplicitDeny` from `NoMatch`, the AWS harness should not rely on a
single policy execution. Use paired policies instead:

1. for allow classification:
   - install only the candidate allow statement
   - use a requester with no other grant path
   - success means the allow matched; `AccessDenied` means there was no allow
2. for deny classification:
   - install a broad control allow statement plus the candidate deny statement
   - if AWS still denies, the deny matched and overrode the allow
   - if AWS allows, the deny was a no-match for that request

That makes the AWS result useful as an evaluator oracle rather than just a
final-auth black box.

### 4. Reproducibility Matters More Than Raw Random Volume

Any generated mismatch must produce a compact artifact containing:

- normalized policy JSON
- request action/resource/principal shape
- request tags and header context
- the local evaluator result
- the AWS-observed result or invariant failure

Randomized coverage without small, reviewable repro artifacts will not be
trusted enough to keep.

## File Layout

Keep the layers separate:

- local generator and metamorphic/differential tests in a dedicated auth test
  module, for example `crates/auth/tests/bucket_policy_differential.rs`
- AWS-backed oracle coverage in
  `crates/s3-diff-tests/tests/bucket_policy.rs`
- optional libFuzzer coverage later in `fuzz/fuzz_targets/auth_bucket_policy.rs`
  once the deterministic generator exists

## Phase 1: Local Generator and Metamorphic Checks

Start with an offline generator using the existing `proptest` dependency in
`crates/auth`.

Deliver:

- a small generator for:
  - actions
  - principals
  - resources
  - condition clauses
  - request tag/header context
- metamorphic checks proving semantically equivalent forms behave the same:
  - `normalized_json()` round-trips without changing evaluation
  - scalar-versus-singleton-array JSON forms are equivalent
  - adding a guaranteed non-matching statement does not change the result
  - equivalent statement expansion preserves result
  - deny precedence is stable under statement reordering
- shrinking that renders a mismatch as compact JSON plus a builder-style request
  snippet

Success criteria:

- the evaluator has generated offline coverage beyond the current handwritten
  examples
- failures print a scenario a reviewer can read without replaying a huge fuzz
  input

Current state:

- [x] Added `crates/auth/tests/bucket_policy_differential.rs` as a dedicated
  local differential test module.
- [x] The Phase 1 generator stays inside the currently supported evaluable
  object-policy subset and renders real policy JSON plus a compact
  `PolicyRequest` builder snippet on failure.
- [x] The initial offline metamorphic checks now cover:
  - `normalized_json()` round-trips preserving evaluation
  - scalar-versus-singleton-array JSON forms preserving evaluation
  - adding a guaranteed non-matching statement preserving evaluation
  - equivalent statement expansion preserving evaluation
  - deny-precedence stability under statement reordering
- [x] Proptest failure persistence is enabled so minimized evaluator drift is
  saved as a regression seed under `crates/auth/tests/`.

## Phase 2: Local Request-Context Differential Matrix

Add a bounded randomized matrix over the request-context fields most likely to
drift from AWS semantics.

Initial focus:

- copy-source plus metadata-directive interactions
- request object tags and existing object tags
- canned ACL and grant headers
- SSE and SSE-C headers
- bucket versus object resource applicability

Deliver:

- generated cross-product tests for the tricky request-context families above
- explicit checks that unsupported condition keys stay outside the evaluable
  subset rather than silently affecting results
- targeted promotion of any discovered drift into fixed unit regressions in
  `bucket_policy.rs`

Success criteria:

- the high-risk request-context combinations are covered by generated tests, not
  just one-off examples
- evaluator drift shows up as a small local failure before AWS-backed suites
  run

Current state:

- [x] Extended `crates/auth/tests/bucket_policy_differential.rs` with a bounded
  Phase 2 request-context matrix.
- [x] Added a generated exactness check over the supported request-context
  families so a policy keyed on one field is unaffected by unrelated
  copy-source, tag, ACL/grant, or SSE/SSE-C request context.
- [x] Added an explicit `copy-source` plus `metadata-directive` combination
  matrix so that pair is covered as a joint condition family rather than only
  incidentally through the general generator.
- [x] Added a local bucket-vs-object resource applicability matrix over
  selected representative actions, which pins the current parse/evaluation
  surface for those modeled rows:
  - representative object actions accept object-scoped resources and reject
    bucket-only resources at parse time
  - representative bucket actions accept bucket resources, require
    bucket-scoped requests, and reject object-scoped resources at parse time
- [x] Added an explicit unsupported-condition guard so known unsupported keys
  like `aws:PrincipalArn`, `aws:SourceVpc`, `aws:SourceVpce`, `aws:SourceIp`,
  and `s3:VersionId` are rejected by
  `validate_evaluable_object_conditions()` for evaluable object actions.

## Phase 3: AWS-Backed Oracle Harness

Add a generated-but-bounded AWS-vs-local differential harness under
`crates/s3-diff-tests`.

Start narrow. The first slice should cover only operations that are easy to
materialize precisely and already have strong harness support:

- `GetObject`
- `PutObject`
- `GetObjectTagging`
- `PutObjectTagging`
- `CopyObject`

Deliver:

- a scenario materializer that:
  - provisions bucket/object/tag state
  - installs generated policy JSON
  - issues the corresponding S3 request with the needed headers or tags
  - classifies the AWS result using the allow/deny pairing strategy above
- explicit filtering so only materializable scenarios reach AWS
- mismatch reporting that includes both the local `PolicyEvaluation` and the
  AWS-observed class

Later expansion can add:

- `DeleteObjectTagging`
- `GetObjectRetention` / `PutObjectRetention`
- `GetObjectLegalHold` / `PutObjectLegalHold`
- selected bucket-scoped actions once the object path is stable

Success criteria:

- at least one generated AWS-backed oracle exists for each major condition
  family we intentionally enforce
- Argmin and AWS disagreements minimize into stable regression tests

Current state:

- [ ] Phase 3 is started but not complete.
- [x] Added an initial AWS-vs-local differential harness at
  `crates/s3-diff-tests/tests/bucket_policy.rs`.
- [x] The harness now reuses the existing `s3-tests` fixture and client setup,
  executes the same scenario against AWS and a local Argmin server, and checks
  the local `BucketPolicy::evaluate` classification against the observed
  allow-versus-reject result for selected object-policy condition families:
  - `s3:ExistingObjectTag/*`
  - `s3:RequestObjectTag/*`
  - `s3:x-amz-copy-source`
  - `s3:x-amz-metadata-directive`
  - `s3:x-amz-acl`
  - representative `s3:x-amz-grant-*` coverage via `s3:x-amz-grant-read`
  - `s3:x-amz-server-side-encryption`
  - `s3:x-amz-server-side-encryption-customer-algorithm`
- [x] The Phase 3 harness now includes paired-policy rows that distinguish
  `ExplicitDeny` from `NoMatch` on the local evaluator side while still
  comparing the live AWS/local server outcome as allow versus reject.
- [x] The differential now isolates each scenario in its own bucket/policy
  surface so AWS oracle rows do not depend on policy replacement convergence
  across scenarios.
- [ ] The expanded AWS-backed run still needs to be rechecked end to end before
  Phase 3 can be marked complete.

## Phase 4: Fuzz Target and Corpus Promotion

Once the deterministic generator and AWS oracle are stable, add a pure offline
fuzz target for evaluator semantics.

Deliver:

- `auth_bucket_policy` under `fuzz/`
- corpus seeds built from:
  - minimized AWS mismatches
  - existing unit regressions
  - wildcard/resource/principal edge cases
- target invariants such as:
  - no panics
  - normalization round-trips preserve meaning
  - equivalent policy encodings preserve meaning

Keep this target evaluator-only. It should not depend on live AWS.

## Validation

Minimum validation for each phase:

1. `cargo test -p auth bucket_policy -- --nocapture`
2. `cargo test -p auth bucket_policy_differential -- --nocapture`
3. AWS-backed runs using the existing credentials and guidance in
   `guides/testing.md`, starting with a dedicated
   `cargo test -p s3-diff-tests --test bucket_policy -- --nocapture`
4. optional offline fuzzing with `cargo fuzz run auth_bucket_policy`

## Success Criteria

- `BucketPolicy::evaluate` has generated coverage, not only example coverage
- the supported condition subset has a live AWS oracle
- any mismatch produces a compact repro artifact and a fixed regression
- unsupported condition keys remain explicit out of scope instead of being
  silently mixed into the generator

## Recommended Order

1. build the offline generator and metamorphic checks
2. add the request-context differential matrix
3. add the narrow AWS-backed oracle for a small supported subset
4. promote mismatches into fixed regressions and only then add a fuzz target
