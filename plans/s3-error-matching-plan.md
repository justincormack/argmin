# S3 Operation Error Matching Plan

Status: Proposed

## Problem

`ServerError` currently combines internal failures with all public S3 errors, and its S3 code
and HTTP status are selected globally. This makes it possible for an operation to return a
public error that is valid somewhere in S3 but impossible for that request. Regressions have
included conditional errors escaping from unconditional operations and storage overload being
reported as `OperationAborted`/409 instead of `SlowDown`/503.

We want an explicit, reviewable contract for the errors each request may expose, without
duplicating that contract throughout storage and coordinator helpers.

## Effective Request Kind And Contract Selection

Associate one static error contract with an **effective request kind** and the relevant parsed
request properties. Exhaustively classify effective request kinds as either:

- `SmithyOperation`, which maps one-to-one to an S3 Smithy operation
- `ProtocolOnlyOperation`, for routed S3 protocol surfaces that have no Smithy operation, such
  as CORS `OPTIONS`

The effective request kind is selected after all routing discriminators have been parsed,
including headers that distinguish Smithy operations sharing one HTTP route. In particular,
routed `PutObject` is refined to Smithy `PutObject` or `CopyObject`, and routed `UploadPart` is
refined to Smithy `UploadPart` or `UploadPartCopy`, before contract selection. Protocol-only
kinds have explicit protocol/AWS-oracle provenance rather than a fabricated Smithy mapping.

Do not pass a caller-selected runtime error set through internal operations. The dispatcher
selects the contract so a helper cannot select a more permissive one. The effective request kind
becomes part of the request execution context and is retained by streaming workers and response
bodies until they terminate.

The contract identifies:

- the effective Smithy operation or explicit protocol-only operation
- the request stage: routing, authentication, validation, operation execution, request-body
  handling, embedded-result rendering, or committed response streaming
- request properties that alter the legal set, initially conditional versus unconditional and
  buffered versus streaming
- the exact public renderer identity, including S3 code, HTTP status, message/body template,
  XML declaration behavior, required headers, and resource/argument fields
- the response channel: ordinary XML, headers-only, `DeleteObjects` per-entry error, an error
  embedded after a successful `CompleteMultipartUpload` response has begun, or committed-stream
  transport abort
- the provenance of each allowed error: Smithy-modeled, common S3 protocol/authentication, or
  observed and pinned by an AWS oracle test

An operation-only set is insufficient. For example, `DeleteObject` can be conditional or
unconditional, and allowing `ConditionalRequestConflict` for the whole operation would not
catch the original class of regression.

`S3WireErrorKind` identifies a complete AWS response shape, not merely a code/status pair. For
example, missing-`uploadId` renderers for `UploadPart` and `UploadPartCopy` remain distinct when
their XML declaration or other body details differ.

## Rendering Boundaries

There is no single final error-rendering boundary today. Inventory every route,
authentication, buffered-dispatch, streaming-prepare, streaming-body, embedded-result, and
response-body exit. Replace direct generic rendering with one private contract-aware rendering
module. Raw error response constructors must not remain callable from request handling code.

Before response headers are committed, the renderer validates the stage-bound error against
the selected contract and renders the exact response shape. After successful headers are
committed, an ordinary S3 error cannot replace the response. A response-body failure then uses
the distinct `CommittedResponseAbort` channel, which records diagnostics and terminates the
stream. AWS protocols that deliberately encode an error in a successful response, such as
`CompleteMultipartUpload`, use an explicit embedded-error channel and renderer instead.

In tests and debug builds, a disallowed pre-commit error is an invariant failure. In production
it emits a structured diagnostic and becomes `InternalError`, rather than being guessed into a
nearby allowed error. A disallowed post-commit failure emits the invariant diagnostic and aborts
the response transport because its status and headers can no longer be replaced.

## Smithy Baseline And AWS Extensions

The AWS S3 Smithy model is a baseline, not a complete wire-error specification. In the pinned
AWS SDK model, operations such as `DeleteObject` and `CompleteMultipartUpload` have no modeled
operation errors even though AWS observably returns authentication, validation, contention,
and conditional errors. These appear to SDK users as unmodeled errors.

Generate and commit an operation/error manifest from a pinned AWS S3 Smithy model. Tests must
require:

1. every routed effective request kind is exhaustively classified as Smithy or protocol-only
2. every Smithy effective operation has exactly one Smithy operation mapping
3. every protocol-only operation has an explicit contract and protocol/AWS-oracle provenance
4. every applicable modeled error is present, or has an explicit unsupported/not-applicable
   annotation
5. every allowed non-modeled error has common-protocol or AWS-oracle provenance
6. no unexplained error is added to an operation contract

The generated manifest is test/build input only and must not add a production Smithy dependency
or require network access in CI.

## Stage-Bound Cause-To-Wire Matching

An allowed-set check cannot distinguish two errors that are both legal for an operation, and
it cannot recover provenance from an already constructed `SlowDown` or `OperationAborted`.
Cause-sensitive wire errors must therefore be unforgeable. Replace freely constructible public
variants with a closed internal cause and an opaque, stage-bound error envelope. Only the
central mapper may turn that cause into an `S3WireErrorKind`; its fields and constructors remain
private to the mapping/rendering module.

The envelope carries:

- the effective request-kind identity
- an authoritative failure-stage token minted by the request transition that owns that stage,
  rather than a caller-supplied stage enum
- the closed internal cause classification and retained diagnostic source
- the mapped exact renderer identity

Every transition into routing, authentication, validation, durable execution, request-body
handling, embedded-result rendering, or committed response streaming supplies its own private
stage token. This proves coarse stage and set membership: an error legal only during execution
cannot be emitted during authentication merely because both appear in the overall contract.

A stage token does **not** prove precedence between checks within one stage. The generic mapper
must not claim otherwise. Where exact same-stage precedence matters, use one of these
operation-specific mechanisms:

- a private failure-point/typestate proof minted only after the prerequisite checks completed
- centralized candidate resolution that receives all relevant request facts and validation
  outcomes, then selects an error from an oracle-pinned partial-order policy

Some AWS precedence is not linear. `CompleteMultipartUpload`, for example, has competing upload
existence, XML, part-order, ETag, checksum, condition, and expected-size failures. Its behavior
must remain pinned by an oracle matrix; only precedence represented by explicit failure-point
evidence or candidate resolution is claimed as runtime-enforced.

The centralized mapping contract initially covers these important internal causes:

| Internal cause | Public result |
| --- | --- |
| metadata command or ordinary request contention | `OperationAborted`/409 |
| overlapping conditional mutation | operation-specific `ConditionalRequestConflict`/409 or `PreconditionFailed`/412 |
| capacity or admission resource exhaustion | `SlowDown`/503 |
| failed request precondition | `PreconditionFailed`/412 |
| corruption, invariant, unclassified transport, database, or storage failure | `InternalError`/500 |

Mappings may be operation- and stage-specific where AWS behavior differs. Internal callers
return closed semantic causes; they cannot construct `SlowDown`, `OperationAborted`, or a
conditional renderer directly. The mapper preserves its diagnostic cause so tests can verify
the cause, authoritative stage, and final wire result. Tests assert exact precedence only when
the envelope also carries operation-specific failure-point evidence.

## Implementation Slices

### 1. Contract And Renderer Inventory

- Introduce stable `EffectiveRequestKind`, `SmithyOperationKind`, `ProtocolOnlyOperationKind`,
  exact-renderer `S3WireErrorKind`, request stage/profile, response-channel, and provenance
  types.
- Inventory every header/query discriminator, prove each Smithy kind maps one-to-one to a Smithy
  operation, and explicitly classify protocol-only kinds.
- Inventory current errors and every direct renderer, including streaming preparation and
  response-body failures after headers have been committed.
- Import the pinned Smithy baseline and record explicit common/oracle extensions.
- Add exhaustive tests so a new route or effective request kind cannot omit its classification
  and contract.

### 2. Closed Causes And Stage Envelopes

- Introduce the closed internal cause classification and opaque mapped-error envelope.
- Make cause-sensitive wire kinds constructible only by the central mapper.
- Mint authoritative private stage tokens at request state transitions and carry the effective
  request kind and stage through streaming workers and response bodies.
- Add compile-time/module-boundary tests preventing direct construction of cause-sensitive wire
  errors or caller-selected stage envelopes.

### 3. Contract-Aware Rendering

- Replace every direct rendering exit with the private contract-aware renderer.
- Give pre-routing failures a separate protocol contract because no operation exists yet.
- Validate `DeleteObjects` item errors and multipart embedded errors at their distinct rendering
  boundaries.
- Treat post-header GET/body failures as committed-response aborts, never replacement HTTP 500
  responses.
- Add negative regressions proving an unconditional request rejects conditional errors and an
  operation cannot return an unrelated public error.
- Mechanically reject new calls to raw error response constructors outside the rendering module.

### 4. Cause Mapping

- Centralize contention, conditional, overload, and internal-failure conversions.
- Add table-driven tests for `(effective request kind, stage, request profile, internal cause) ->
  exact renderer identity`.
- Specifically pin metadata contention as 409 and resource exhaustion as 503 wherever AWS
  permits those outcomes.

### 5. Operation-Specific Precedence

- Inventory operations where multiple legal failures compete within one coarse stage.
- Preserve existing AWS oracle matrices as the source of truth, including non-linear outcomes.
- Add private failure-point proofs or centralized candidate resolution only where the runtime
  implementation can establish the required prerequisite facts.
- Do not describe oracle-tested precedence as runtime-enforced unless the corresponding evidence
  is carried to the mapper.

### 6. Typed Hotspot Results

After closed cause mapping and runtime enforcement are complete, further narrow broad semantic
cause results into operation-specific result enums on mutation paths that have produced
recurring mistakes:

- `PutObject`, `DeleteObject`, and `DeleteObjects`
- `CopyObject`
- multipart creation, upload, copy, completion, and abort
- conditional object metadata/tag/ACL mutations

Shared internal helpers may continue to return closed semantic internal causes. Conversion into
an exact public renderer occurs once through the central mapper. Operation-specific result enums
for all read-only operations are optional, but the closed cause envelope and contract-aware
renderer are mandatory for every request path.

## Completion Criteria

- Every routed effective request kind is exhaustively classified and has a contract.
- Every Smithy operation kind maps one-to-one to Smithy; protocol-only kinds such as CORS
  `OPTIONS` have explicit protocol/oracle provenance without a Smithy mapping.
- `CopyObject` and `UploadPartCopy` select contracts distinct from their shared HTTP routes.
- The pinned Smithy comparison and all explicitly annotated extensions are mechanically tested.
- Every rendering exit, including streaming and committed response bodies, is inventoried and
  contract-aware.
- Cause-sensitive renderer identities and failure stages cannot be caller-constructed.
- Conditional errors cannot escape from an unconditional request profile.
- Resource exhaustion cannot be rendered as ordinary metadata contention, and vice versa.
- Errors are checked against their authoritative coarse stage and exact renderer shape,
  including XML/header differences.
- Same-stage AWS precedence remains pinned by oracle matrices and is runtime-enforced only where
  operation-specific failure-point evidence or candidate resolution proves it.
- Disallowed pre-commit errors fail loudly in tests and fail closed with diagnostics in
  production; disallowed post-commit errors terminate the transport with diagnostics.
- AWS oracle tests remain authoritative for observed behavior missing or contradicted by the
  Smithy model.
