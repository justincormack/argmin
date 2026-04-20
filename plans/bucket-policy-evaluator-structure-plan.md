# Bucket Policy Evaluator Structure Plan

Status: planned

## Scope

Restructure the bucket-policy evaluator in `crates/auth/src/bucket_policy.rs`
so that condition operators become data rather than hand-rolled match
statements, and prepare a small evaluator seam that IAM work can plug into
later without replaying the same growth problem.

In scope:

- a table-driven representation of condition operators and their shared
  `IfExists` / negation semantics
- a small evaluator seam (`PolicyEvaluator`, `PolicyDecision`) that can host
  bucket policy today and additional policy sources later
- regression-first migration: existing unit, differential, and fuzz tests keep
  passing at each step
- explicit per-operator unit coverage and proptest hooks

Out of scope:

- introducing a third-party policy engine (Cedar, Oso, Polar, datalog)
- widening condition-key support beyond what the current evaluator accepts
- writing an IAM evaluator in this plan; this plan only prepares the seam
- changing externally visible policy semantics; this is structural only
- rewriting the policy parser (`parse_bucket_policy`)

## Why This Is Worth Doing

The evaluator is growing along two axes that scale differently, and the
current code treats them the same way:

1. **Condition operators.** Each new operator (`IpAddress`, `NotIpAddress`,
   `NumericLessThan`, `ArnEquals`, `ArnLike`, `DateEquals`, `Bool`, …) is
   individually small and independently testable. AWS has roughly 25
   operators; `bucket_policy.rs` currently implements a subset.

2. **Policy-source composition.** Bucket policy, IAM identity policy,
   permission boundaries, session policies, SCPs, and resource policies each
   evaluate separately and combine with AWS-specific rules (explicit-deny
   precedence, default deny, cross-account carveouts, service-linked roles).
   AWS's evaluation logic is non-trivial and genuinely intricate.

Axis 1 scales as O(operators) in code when done well and O(operators × sites)
today because several call sites re-enumerate the same operator list:

- `string_condition_matches` (`bucket_policy.rs:1438`) dispatches on operator
  for the main evaluator path.
- `string_equals_condition_matches` (`bucket_policy.rs:1416`) duplicates the
  `StringEquals` arm for the `ExistingObjectTag/*` fast path.
- `evaluable_string_condition_operator_supported` (`bucket_policy.rs:1514`)
  enumerates the same operator set again as a supportedness predicate.
- `split_if_exists_operator` (`bucket_policy.rs:1528`) threads `IfExists`
  through each call site individually instead of being a property of an
  operator definition.

Each new operator has to be added in all of these places, with the
temptation to forget one. The condition-operator table in this plan collapses
them.

Axis 2 is not in scope to implement now, but the shape of bucket-policy
evaluation today quietly baked in the single-source assumption. The second
phase of this plan introduces a thin `PolicyEvaluator` seam so the IAM plan
can add a second source behind the same combinator, with deny-precedence and
default-deny implemented exactly once as a testable unit.

## Design Principles

### 1. Keep AWS as the oracle

No semantic change. The existing unit tests in `bucket_policy.rs`, the
differential tests in `crates/auth/tests/bucket_policy_differential.rs`, the
`auth_bucket_policy` fuzz target, and the AWS-backed `s3-tests` suites all
stay green at every step.

If the restructure and a test disagree, the test is right.

### 2. No new runtime dependencies

Do not pull in Cedar, Oso, Polar, Soufflé, Crepe, Ascent, or any other policy
engine. The evaluator remains hand-rolled. The refactor's value is in shape,
not in delegation.

Cedar specifically is rejected here: AWS IAM is not Cedar, AWS's own
Verified Permissions translates between the two, and the project's standing
rule (`AGENTS.md`) is to exactly match AWS S3 behavior. A second formal
semantics in the loop makes that harder, not easier.

### 3. One definition per operator, one definition per condition key

Each condition operator should live in one place, own its own negation and
`IfExists` semantics, and expose a uniform evaluation contract. The
supportedness predicates (which operators are valid for which actions) should
be derived from the operator table, not re-enumerated at the use site.

Each condition key (`s3:ExistingObjectTag/*`, `s3:x-amz-acl`,
`aws:SourceIp`, …) should resolve to an `Option<&str>` (or typed value for
non-string keys) in exactly one place, and the evaluator should apply the
operator table to that resolved value.

### 4. Result type stays five-valued

`ConditionMatchResult` (`bucket_policy.rs:739`) already distinguishes
`Matches`, `NoMatch`, `AcceptedButNotEvaluable`, `InputUnavailable`, and
`Unsupported`. Keep that exact enum; the refactor routes through it rather
than replacing it.

### 5. Internal restructure, not external API change

`BucketPolicy`, `PolicyEvaluation`, `PolicyRequest`, `PolicyAction`,
`PolicyEffect`, and the `parse_bucket_policy` entry point stay stable.
Callers in `server-core` should not notice this work at all.

## Phase 1: Extract a Condition Operator Table

Introduce a small trait and a compile-time table. The goal is that adding a
new operator is one enum variant plus one table row, with the supportedness
predicate derived from the table.

### File Layout

Add a new module within `crates/auth`:

- `crates/auth/src/bucket_policy/mod.rs` (move current
  `bucket_policy.rs` here as a single-file-module or split; prefer the
  smallest mechanical move at first)
- `crates/auth/src/bucket_policy/condition_op.rs`

The second file holds the operator trait, the operator enum, the table, and
per-operator unit tests.

### Operator Contract

Approximate shape; adjust names during review:

```rust
pub(crate) enum ConditionOpKind {
    StringEquals,
    StringNotEquals,
    StringLike,
    StringNotLike,
    Null,
    IpAddress,          // added in a later commit
    NotIpAddress,       // added in a later commit
    // ...
}

pub(crate) struct ConditionOpDef {
    pub(crate) name: &'static str,
    pub(crate) kind: ConditionOpKind,
    pub(crate) supports_if_exists: bool,
    pub(crate) negated: bool,
    pub(crate) value_kind: ConditionValueKind,
    pub(crate) evaluate: fn(&[String], ActualValue<'_>) -> ConditionMatchResult,
    pub(crate) evaluable_on_evaluable_object_actions: bool,
}

pub(crate) const CONDITION_OPS: &[ConditionOpDef] = &[ ... ];
```

`ActualValue` captures the three input states the evaluator already cares
about (`Present(&str)`, `Absent`, `Unavailable`) so the per-operator function
does not re-implement that branching.

### Responsibilities

- lookup by name (operator string → `ConditionOpDef`)
- lookup honors `IfExists` suffix handling in one place, using
  `supports_if_exists`
- each operator's `evaluate` is a small pure function taking resolved
  operand values and the actual condition-key value
- `Unsupported` is returned when a name is not in the table
- the supportedness predicate
  (`evaluable_string_condition_operator_supported`) becomes a filter over
  `CONDITION_OPS`

### Migration Steps

1. Add the module and the empty table. No call sites change yet; the module
   is dead code behind `#[allow(dead_code)]`.
2. Fill in `StringEquals`, `StringNotEquals`, `StringLike`, `StringNotLike`,
   and `Null`, covering the current subset. Add per-operator unit tests in
   the new module.
3. Route `string_condition_matches` through the table; delete the inner
   per-operator arms. Confirm the full workspace tests still pass.
4. Route `string_equals_condition_matches` through the table; delete it.
5. Replace `evaluable_string_condition_operator_supported` with a derived
   helper over the table.
6. Remove `split_if_exists_operator` in favor of table-driven handling.

Each step should land as a separate commit, with the full workspace test run
in between. The refactor is rigidly test-first: the existing tests define
the contract.

### Success Criteria

- one place to look when reviewing "does this operator do what AWS does"
- one place to add a new operator, with a required unit test in the same
  module
- no duplicated operator enumeration in predicates or fast paths
- existing unit, differential, and fuzz coverage unchanged and passing

## Phase 2: Extract a Condition-Key Resolver

Replace the hand-written dispatch in `condition_clause_matches_request`
(`bucket_policy.rs:1311`) with a small table that maps condition keys to
value resolvers on `PolicyRequest`.

### Shape

```rust
struct ConditionKeyResolver {
    name: &'static str,                  // for exact-match keys
    prefix: Option<&'static str>,        // for "s3:ExistingObjectTag/" style
    resolve: fn(&PolicyRequest<'_>, &str) -> ResolvedValue<'_>,
    evaluable_predicate: Option<fn(PolicyAction) -> bool>,
}
```

`ResolvedValue` captures the existing `ExistingObjectTagValue::Unavailable`
branch so that the evaluator does not have to special-case tag resolution.

The condition-key table is deliberately separate from the operator table.
Operator semantics are universal; condition-key semantics are S3-specific and
will be extended when IAM keys arrive (`aws:PrincipalType`,
`aws:SourceAccount`, etc.). Splitting them avoids entangling "how to
evaluate an operator" with "what `s3:ExistingObjectTag/foo` means in this
request context".

### Success Criteria

- adding a new condition key is one table row plus one resolver function
- `AcceptedButNotEvaluable` / `InputUnavailable` behavior remains tied to
  the condition key, not scattered through operator code
- no behavior change to current tests

## Phase 3: Introduce a Policy Evaluator Seam

Add a small trait and decision type so bucket policy becomes one implementor
among several.

### Shape

```rust
pub(crate) struct PolicyDecision {
    effect: Option<PolicyEffect>,
    source: PolicySourceKind,
}

pub(crate) trait PolicyEvaluator {
    fn evaluate(&self, request: &PolicyRequest<'_>) -> PolicyDecision;
}
```

`BucketPolicy` implements `PolicyEvaluator` by wrapping its current
`evaluate` method; the return type converts from `PolicyEvaluation` to
`PolicyDecision`. Existing public API (`BucketPolicy::evaluate`) is kept as
the stable entry point.

A single combinator function (start with one: combine a list of
`PolicyDecision` values using AWS's explicit-deny-wins and default-deny
rules) is added with its own unit tests. Today that function has exactly
one input, so the combinator is trivial; it exists so that when IAM lands,
the deny-precedence logic already has a home and a regression suite.

### Non-goals for this phase

- do not build the IAM evaluator
- do not wire multiple sources into `server-core`
- do not change how the coordinator calls bucket policy

This phase is purely a shape move: it gives IAM a place to attach without
surprising the bucket-policy code path.

### Success Criteria

- `BucketPolicy::evaluate` still exists and still returns `PolicyEvaluation`
- `PolicyEvaluator` and `PolicyDecision` exist with direct unit tests
- combinator tests pin the AWS evaluation rules in a way that survives
  adding a second policy source
- no changes in `server-core`

## Phase 4: Hook the IAM Plan In Later

This phase is listed for continuity only and is not part of this plan's
delivery.

When IAM work starts, it should:

- add a new `IamPolicy` type implementing `PolicyEvaluator`
- extend the condition-key table with IAM-specific keys where supported
- extend the combinator from phase 3 to accept multiple policy sources,
  with explicit source ordering matching AWS
- reuse the same differential test and fuzz infrastructure introduced by
  `plans/completed/bucket-policy-differential-testing-plan.md`

If the seam from phase 3 is shaped wrong, fix it then; the point of phase 3
is to make that a localized fix, not a second evaluator rewrite.

## Test Plan

Each phase must leave all existing suites passing:

- `cargo test -p auth`
- `cargo test -p auth --test bucket_policy_differential`
- targeted authz coverage: `cargo test -p server-core authz_model_`
- `./scripts/security-tests authz`
- `./scripts/fuzz auth_bucket_policy --time 2m` (manual spot check at least
  once per phase)

New coverage added by this plan:

- phase 1: per-operator unit tests in `condition_op.rs`
- phase 1: a proptest that evaluates every operator against a generated
  actual-value shape to confirm `Matches` / `NoMatch` / input-absent
  branches are covered
- phase 2: per-resolver unit tests for each condition key
- phase 3: combinator unit tests covering the AWS rules
  (explicit-deny-wins, no-match-default-deny, allow-then-deny, deny-only,
  allow-only, no-policy)

## Risks

### 1. Sneaking in behavior changes during mechanical moves

Mitigation: land each step as its own commit; keep the existing differential
and fuzz tests green at every step; review diffs line-by-line on the two
arms of `string_condition_matches` specifically, because those are the
semantic-change-prone spots.

### 2. Over-engineering phase 3

Mitigation: phase 3 has exactly one implementor and a trivial combinator at
first. If it starts growing a policy-source registry or dynamic dispatch,
stop and revisit. The point is shape, not abstraction.

### 3. The table becomes its own growing surface

Mitigation: keep the operator and condition-key tables in compile-time
`&[...]` slices with `const` entries, not runtime registries. New
operators/keys land as PRs that touch one table row and one unit test.

### 4. `IfExists` semantics are subtly per-operator

Mitigation: encode `supports_if_exists` as a boolean on the definition and
test each operator's `IfExists` variant explicitly. Do not silently derive
`IfExists` for every operator; AWS is not consistent here.

## Success Criteria

- condition operators live in one place with per-operator unit tests
- condition keys live in one place with per-key resolver tests
- supportedness predicates are derived, not re-enumerated
- a `PolicyEvaluator` seam exists, implemented by `BucketPolicy`, with a
  tested decision combinator ready for a second policy source
- no change to `BucketPolicy::evaluate`'s external contract
- existing differential, unit, integration, and fuzz coverage all pass
  unchanged
- the IAM plan, when it is written, can add a second evaluator without
  touching bucket-policy semantics

## Recommended Order

1. phase 1 commits (operator table, migration, cleanup)
2. phase 2 commits (condition-key table)
3. phase 3 commits (evaluator seam and combinator tests)
4. hand off to the IAM plan when it is scoped
