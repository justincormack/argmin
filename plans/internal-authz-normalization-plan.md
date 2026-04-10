# Internal Authz Normalization Plan

## Status

Pending.

Public S3 handlers now consistently use operation-specific `authorize_*`
entrypoints with typed authorized values. The remaining cleanup is internal to
`crates/server-core/src/coordinator/authz.rs`: shared bucket/object auth
helpers, object lock-and-authorize helpers, and the bucket-policy predicate
layer they sit on.

## Context

The recent auth-token refactor established the right public shape:

1. one auth entrypoint per S3 operation
2. operation-specific validation in the auth path
3. typed authorized values flowing into mutation/read helpers

That refactor is now effectively complete at the handler level. What remains is
historical layering inside `authz.rs`, where some helpers still combine:

1. bucket summary loading
2. bucket policy caching/loading
3. object locking
4. ACL/ownership fallback evaluation
5. operation-specific error shaping

This works, but it makes some shared helpers behave like hidden operation
contracts instead of reusable internal building blocks.

## Goals

1. separate raw state loading/locking from authorization decisions
2. make authorized object token/result shapes more uniform
3. reduce duplicated bucket-policy evaluation structure without collapsing the
   public `authorize_*` entrypoints into generic APIs
4. preserve exact AWS behavior, especially error masking and explicit deny
   precedence
5. improve direct auth testability of internal decision points where that adds
   value

## Non-Goals

1. changing the public coordinator API shape again
2. changing storage interfaces or persistence
3. broad policy-engine redesign outside `server-core`
4. collapsing all auth into one generic authorization function

## Workstreams

### 1. Normalize object read authorization internals

Current shared helpers like `lock_object_for_authorized_read` and
`lock_object_for_authorized_read_with_policy` bundle object fetch/lock with
auth evaluation.

Refactor toward three layers:

1. raw object state loading/locking helpers
2. pure auth evaluators over already-loaded state
3. per-operation `authorize_get_*` functions that compose those pieces

Target operations:

1. `GetObject`
2. `HeadObject`
3. `GetObjectRange`
4. `GetObjectPart`
5. `GetObjectAttributes`

Expected outcome:

1. object read auth entrypoints stay explicit
2. shared internals stop carrying operation-specific semantics implicitly
3. error masking remains owned by the operation-level auth functions

### 2. Normalize object ACL/tagging/object-lock helper families

Current helpers such as `lock_object_for_authorized_tagging`,
`lock_object_for_authorized_acl`, and `lock_object_for_authorized_object_lock`
still mix locking, policy lookup, and auth decisions in ways that differ across
families.

Refactor them into a more regular internal model:

1. load and lock object state
2. compute any needed bucket-policy context
3. evaluate operation-specific policy/ACL/ownership rules
4. return a typed authorized token with the locked state needed for apply/read

The public operation entrypoints should remain as they are today:

1. `authorize_get/put/delete_object_tags`
2. `authorize_get/put_object_acl`
3. `authorize_get/put_object_retention`
4. `authorize_get/put_object_legal_hold`

### 3. Consolidate internal bucket-policy evaluation structure

The `requester_can_*_with_bucket_policy` family is correct but repetitive. The
main duplication is structural:

1. identify the policy action/resource
2. evaluate explicit deny / allow against policy context
3. combine with ACL / ownership / same-account fallback rules

Refactor this into a smaller internal substrate:

1. explicit internal evaluators for bucket actions and object actions
2. operation-specific wrappers that preserve today’s names where still useful
3. shared handling of explicit deny, allow, and fallback behavior

This should remain internal to `authz.rs`. The public `authorize_*` entrypoints
should continue to name the S3 operation directly.

### 4. Normalize authorized token families

The bucket side is now fairly consistent, but object-side authorized values
still reflect older layering.

Converge toward a smaller set of internal token categories, for example:

1. authorized object read
2. authorized object write
3. authorized object ACL read/write
4. authorized object tagging read/write
5. authorized object lock read/write
6. authorized multipart destination write

This does not require a single universal token type. The aim is predictable
shapes and naming, not maximum genericity.

## Suggested Order

1. object read helper normalization
2. object ACL/tagging/object-lock helper normalization
3. bucket-policy evaluator consolidation
4. token naming/shape cleanup that falls out of the earlier steps

This order keeps the highest-reuse helper family first and avoids mixing broad
mechanical renames with behavior-sensitive refactors.

## Testing

For each slice:

1. keep existing operation-level tests passing unchanged
2. add direct auth tests only where the refactor exposes a meaningful decision
   boundary
3. preserve masked-not-found vs access-denied behavior with explicit tests
4. run:
   `cargo fmt`
   `cargo clippy --all-targets --all-features -- -D warnings`
   `cargo test --workspace --no-fail-fast`

If any slice changes AWS-conformance assumptions, add or update `s3-tests`
coverage rather than relying only on coordinator-unit tests.

## Risks

1. accidentally changing masked error behavior for unauthorized object reads
2. changing deny-vs-allow precedence when consolidating policy evaluation
3. moving too much logic into generic helpers and recreating the abstraction
   leak in a different form
4. introducing refactors that are mechanically consistent but harder to review

## Success Criteria

This plan is complete when:

1. public `authorize_*` entrypoints remain one-per-operation
2. internal helpers have a clearer split between loading, evaluating, and
   applying
3. shared policy evaluation structure is less repetitive
4. no public coordinator handlers depend directly on legacy generic auth helper
   shapes
5. workspace tests remain clean with no AWS-behavior regressions
