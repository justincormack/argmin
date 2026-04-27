## Authz Model Stateful Follow-up

Status: in progress

This is a deliberately small follow-up to
`plans/completed/authz-model-testing-plan.md`.

Note: explicit extraction of the BOE modern-auth evaluation seam is tracked
separately in `plans/completed/boe-modern-auth-seam-plan.md`. This stateful follow-up
assumes that seam as an input where relevant; it is not the plan for creating
it.

The fixed-matrix authz model work is complete. What remains, if we want to
push further, is a bounded stateful layer that checks short traces across the
same AWS-pinned rules instead of only point-in-time scenarios.

This follow-up is intentionally narrowed to the explicit BOE modern-auth seam.
It is not a plan for legacy ACL or mixed authz state traces.

## Goal

Add a bounded `proptest` trace model for short BOE modern-auth state
transitions without turning the suite into an unreviewable state-machine
project.

The goal is to catch regressions where individually-correct operations compose
incorrectly across changes in:

- bucket policy
- public access block
- bucket ABAC and bucket tags
- versioning and object-lock state
- delete-marker and missing-object discovery behavior

This should stay intentionally small and reviewable. It is not a general
randomized fuzzer for all S3 behavior.

## Scope

Bounded generated operations may include:

- set public-access-block
- set or delete a narrow bucket policy
- enable or disable bucket ABAC
- replace bucket tags with a small fixed tag set
- put object with BOE-compatible request shape
- change bucket versioning state
- delete object or object version with bounded object-lock state
- probe modern read/write/delete actions

The initial trace depth should stay short, for example 2-6 operations, with a
small number of identities and object keys.

## Non-goals

- do not attempt full end-to-end AWS differential state exploration here
- do not replace the explicit fixed matrices with generated coverage
- do not generate large bodies, multipart payloads, or unbounded policy
  documents
- do not generate ACL mutations or ACL-sensitive request-context transitions
- do not broaden the identity surface beyond the existing small owner /
  same-account-admin / cross-account fixtures unless a concrete gap requires it

## Phase 1: Seeded Trace Skeleton

Build a small trace harness inside
`crates/server-core/src/coordinator/authz_model_tests.rs` or a nearby follow-up
module that can:

- materialize a BOE bucket and one or two object seeds
- apply a short sequence of bounded state mutations
- run one probe authorization/read/write action at the end
- classify the result into the same compact outcome enums used by the fixed
  matrices

Acceptance criteria:

- generated traces are deterministic under a fixed seed
- failure output prints the full trace in a reviewable form
- the first trace family is small enough that failures can be reasoned about
  locally without replay infrastructure
- Implemented: `phase12` BOE modern-read trace harness over bounded policy /
  `RestrictPublicBuckets` / bucket-ABAC / bucket-tag mutations with
  `GetObject` and `GetObjectAttributes` probes

## Phase 2: BOE / Policy / Bucket-Tag Interaction Traces

Add the first generated family around the historically tricky transitions:

- `RestrictPublicBuckets`
- narrow bucket-policy allow / deny
- bucket ABAC enable / disable
- bucket-tag-conditioned bucket policy
- bucket tag mutation between matching and non-matching values

The probe actions should focus on modern reads and discovery first, because
those are where the existing fixed matrices already document the most subtle
BOE distinctions.

Acceptance criteria:

- generated traces preserve the explicit AWS-compatible interaction between
  bucket-tag-conditioned policy, `RestrictPublicBuckets`, and BOE fallback
- BOE `GetObject` vs `GetObjectAttributes` remains encoded as distinct expected
  behavior, not collapsed into one generic read rule
- Implemented initial generated family for BOE modern reads, `PutObject`,
  `CreateMultipartUpload`, and `DeleteObject`; versioning and object-lock
  traces remain pending

## Phase 3: Delete/Object-lock and Versioning Traces

Expand to short traces that mix:

- versioning changes
- object-lock retention / legal-hold state
- delete current vs version-specific authorization operations

This phase should target cases where a state change followed by a write or
delete probe could silently regress even if the individual fixed matrices still
pass.

Acceptance criteria:

- BOE modern delete/read behavior remains stable across versioning and
  delete-marker transitions
- governance-bypass and missing-version behavior stays aligned with the fixed
  Phase 7 / 7A rules
- Implemented initial `phase13` BOE versioned-delete authorization trace covering:
  - current read after owner-created delete markers
  - current vs specific-version delete authorization probes
  - governance/compliance/legal-hold effects on specific-version delete authorization
  - bypass-governance behavior against missing and locked versions in authorization
- Remaining gap: explicit versioning-mode transitions themselves are still not
  generated; the current Phase 3 slice starts from a versioning-enabled BOE
  bucket
- Remaining gap: Phase 13 does not yet exercise `delete_object(...)` execution
  and `apply_authorized_delete_object(...)` mutation/recheck behavior; it only
  covers the BOE authorization decision path

## Verification

For each phase:

- run `cargo fmt --all`
- run `cargo clippy --all-targets --all-features -- -D warnings`
- run the targeted `server-core` authz model tests
- run `/bin/bash -lc 'S3_TEST_TIMEOUT_SECS=30 cargo test --workspace --no-fail-fast'`

If a generated family exposes a rule that is not already AWS-pinned, add or
update a narrow AWS-facing `s3-tests` regression before relying on the new
generated expectation.
