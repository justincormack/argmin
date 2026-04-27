# BOE Modern Auth Seam Plan

Status: planned

## Goal

Create an explicit bucket-owner-enforced (`BOE`) "modern auth" evaluation seam
for the implemented authorization surface, separate from the read fast path and
separate from legacy ACL-based evaluation.

The goal is not to cache more operations. The goal is to make BOE semantics a
first-class, directly testable authorization layer that can be exercised
consistently for reads and non-read operations.

## Why This Is Separate

This is not the same project as
`plans/authz-model-stateful-followup-plan.md`.

That plan is about short generated traces over an already-established authz
model. This plan is about extracting and naming the BOE modern-auth decision
surface so that:

- model testing can target BOE semantics directly
- read fast-path behavior is no longer coupled to the only explicit "modern
  auth" path
- non-read BOE operations can use the same semantic layer without introducing
  cache-backed execution paths

## Scope

In scope:

- define an explicit BOE modern-auth decision seam for implemented operations
  where BOE materially changes authorization or request validity
- keep this seam usable from both:
  - the existing BOE read fast path
  - normal snapshot-backed coordinator paths
- expand model testing so BOE modern-auth expectations are checked directly for
  more than just object reads
- preserve the current rule that non-read operations continue to load real
  bucket/object state and do not become cache-backed fast paths

Out of scope:

- adding cache-backed write or delete paths
- replacing the existing explicit authz matrices with generated traces
- broad authz refactors unrelated to BOE modern-auth semantics

## Principles

### 1. Separate semantics from data source

The BOE modern-auth evaluator should be a semantic decision layer, not a cache
policy.

The same decision logic should be callable with:

- cached BOE read summaries for the read fast path
- real coordinator snapshots for normal paths

### 2. Keep ACL fallback explicit

BOE is the simplification boundary: once bucket-owner-enforced is set, ACL
handling is disabled and the evaluator should stay entirely in the modern auth /
config layer.

The intended decision shape is therefore small:

- `Allow`
- `Deny`

The other ownership modes (`BucketOwnerPreferred` and `ObjectWriter`) remain on
the existing snapshot-backed paths that still incorporate legacy ACL behavior.

### 3. Do not broaden the fast path

The read fast path is an optimization boundary.

This plan should not move non-read BOE operations onto cached bucket summaries.
For writes, deletes, multipart mutation paths, ACL/tagging changes, and similar
operations, the BOE modern-auth seam should run on top of normal snapshot-backed
state.

### 4. Use AWS-backed tests as the external oracle

If extracting the seam exposes uncertainty about BOE behavior for a specific
operation, add or refine the relevant AWS-backed `s3-tests` case before relying
on local model expectations.

## Phase 1: Name the BOE Modern Auth Surface

Document the request families that should have an explicit BOE modern-auth
decision path.

Initial target families:

- `GetObject`
- `HeadObject`
- `GetObjectAttributes`
- `PutObject`
- streaming `PutObject`
- `CreateMultipartUpload`
- `UploadPart`
- `CompleteMultipartUpload`
- `AbortMultipartUpload`
- `DeleteObject`

Acceptance criteria:

- the plan names which operations are intended to use the seam
- read fast-path use remains explicit
- non-read operations are explicitly documented as snapshot-backed

## Phase 2: Extract Shared BOE Evaluators

Refactor coordinator authorization so BOE modern-auth evaluation is callable as
an explicit step for the target families, without requiring the read fast path.

Acceptance criteria:

- BOE reads still use the fast path where they do today
- the same BOE evaluator can be called from non-read paths with real state
- non-BOE ownership modes remain on the existing legacy/mixed control flow

## Phase 3: Expand Direct BOE Model Testing

Add focused authz model coverage that treats BOE modern-auth as its own subject
rather than only an aspect of the read fast path.

Priority additions:

- BOE write-family matrices for `PutObject` / streaming `PutObject`
- BOE multipart write/manage matrices for the implemented multipart operations
- BOE delete/missing-object discovery checks where behavior differs from
  ordinary ACL-governed buckets
- invariants that the BOE read fast path matches the BOE snapshot-backed
  evaluator for the same request

Acceptance criteria:

- BOE modern-auth tests are clearly distinguishable from legacy ACL matrices
- read fast-path correctness is checked against the explicit BOE evaluator
- non-read BOE expectations no longer depend on read-fast-path-specific wiring

## Verification

For each implementation slice:

- run `cargo fmt --all`
- run `cargo clippy --all-targets --all-features -- -D warnings`
- run targeted `server-core` authz model tests
- run the relevant AWS-backed `s3-tests` BOE / ownership / multipart / object
  lock cases for any operation whose expected BOE behavior is newly pinned

Before committing a substantial slice:

- run `cargo nextest run`
  or `/bin/bash -lc 'S3_TEST_TIMEOUT_SECS=30 cargo test --workspace --no-fail-fast'`
  as appropriate for the change size and repository policy
