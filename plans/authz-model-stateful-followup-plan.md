## Authz Model Stateful Follow-up

Status: planned

This is a deliberately small follow-up to
`plans/completed/authz-model-testing-plan.md`.

The fixed-matrix authz model work is complete. What remains, if we want to
push further, is a bounded stateful layer that checks short traces across the
same AWS-pinned rules instead of only point-in-time scenarios.

## Goal

Add a bounded `proptest` trace model for short authz-relevant state
transitions without turning the suite into an unreviewable state-machine
project.

The goal is to catch regressions where individually-correct operations compose
incorrectly across changes in:

- ownership controls
- public access block
- bucket policy
- ACL provenance-sensitive request context
- versioning and object-lock state
- delete-marker and missing-object discovery behavior

This should stay intentionally small and reviewable. It is not a general
randomized fuzzer for all S3 behavior.

## Scope

Bounded generated operations may include:

- set ownership controls
- set public-access-block
- set or delete a narrow bucket policy
- put object with a constrained ACL shape
- copy object with constrained destination headers
- change object ACL
- update bucket ACL with constrained request context
- change bucket versioning state
- delete object or object version with bounded object-lock state
- read or mutate object tags

The initial trace depth should stay short, for example 2-6 operations, with a
small number of identities and object keys.

## Non-goals

- do not attempt full end-to-end AWS differential state exploration here
- do not replace the explicit fixed matrices with generated coverage
- do not generate large bodies, multipart payloads, or unbounded policy
  documents
- do not broaden the identity surface beyond the existing small owner /
  same-account-admin / cross-account fixtures unless a concrete gap requires it

## Phase 1: Seeded Trace Skeleton

Build a small trace harness inside
`crates/server-core/src/coordinator/authz_model_tests.rs` or a nearby follow-up
module that can:

- materialize a bucket and one or two object seeds
- apply a short sequence of bounded state mutations
- run one probe authorization/read/write action at the end
- classify the result into the same compact outcome enums used by the fixed
  matrices

Acceptance criteria:

- generated traces are deterministic under a fixed seed
- failure output prints the full trace in a reviewable form
- the first trace family is small enough that failures can be reasoned about
  locally without replay infrastructure

## Phase 2: BOE / Public-ACL / Policy Interaction Traces

Add the first generated family around the historically tricky transitions:

- ACL/public-read object or bucket state
- `IgnorePublicAcls` vs `BlockPublicAcls`
- object ownership mode transitions
- narrow bucket-policy allow / deny

The probe actions should focus on reads and discovery first, because those are
where the existing fixed matrices already document the most subtle rules.

Acceptance criteria:

- generated traces preserve the explicit AWS-compatible distinction between
  `IgnorePublicAcls` and `BlockPublicAcls`
- BOE `GetObject` vs `GetObjectAttributes` remains encoded as distinct expected
  behavior, not collapsed into one generic read rule

## Phase 3: Request-context and Delete/Object-lock Traces

Expand to short traces that mix:

- provenance-sensitive request-context writes
- versioning changes
- object-lock retention / legal-hold state
- delete current vs version-specific operations

This phase should target cases where a state change followed by a write or
delete probe could silently regress even if the individual fixed matrices still
pass.

Acceptance criteria:

- ACL/tagging request-context exactness is preserved across state transitions
- governance-bypass and missing-version behavior stays aligned with the fixed
  Phase 7 / 7A rules

## Verification

For each phase:

- run `cargo fmt --all`
- run `cargo clippy --all-targets --all-features -- -D warnings`
- run the targeted `server-core` authz model tests
- run `/bin/bash -lc 'S3_TEST_TIMEOUT_SECS=30 cargo test --workspace --no-fail-fast'`

If a generated family exposes a rule that is not already AWS-pinned, add or
update a narrow AWS-facing `s3-tests` regression before relying on the new
generated expectation.
