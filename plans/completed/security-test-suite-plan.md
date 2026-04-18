# Security Test Suite Plan

## Status

Completed for the intended local deterministic scope.

Prerequisite security-hardening plans that this plan was originally waiting on
have now largely completed. This plan should treat them as input sources for
existing coverage, not as still-active dependencies:

- `plans/completed/authz-model-testing-plan.md`
- `plans/completed/bucket-policy-differential-testing-plan.md`
- `plans/completed/multipart-reclaim-stream-session-testing-plan.md`
- `plans/completed/parser-hardening-plan.md`

Initial work now landed:

- `scripts/security-tests` exists as the first local deterministic runner
- `guides/security-testing.md` exists as the checked-in surface matrix
- `guides/security-findings-inventory.md` now maps every file under
  `security/` to a local regression path or explicit invalid/stale status
- the runner now has dedicated `lifecycle`, `redaction`, and `integrity`
  groups in addition to the earlier auth/parser/stateful surfaces

Remaining optional follow-on:

- a small report wrapper beyond the sectioned `scripts/security-tests` output
- CI wiring, if the project later decides it wants a separate CI signal

## Scope

This plan defines a separate security-focused local test suite and reporting
path for the repository.

It is intentionally distinct from:

- `./scripts/coverage`, which measures local integration test coverage through
  `s3-tests`
- AWS-backed compatibility and differential suites such as `s3-tests` and
  `s3-diff-tests`
- parser fuzzing and other long-running robustness jobs

In scope:

- a curated local, deterministic security test runner
- threat-model-driven grouping of security-sensitive tests
- a checked-in security test matrix mapping surfaces, findings, and plans to
  concrete test commands
- per-surface reporting based on coverage of security behaviors, not line
  coverage percentage
- a place to land future security-focused local tests from the authz,
  bucket-policy, parser-hardening, and multipart/reclaim/session plans

Out of scope:

- replacing the full test matrix
- replacing `./scripts/coverage`
- treating AWS-backed differential tests as local coverage
- inflating small transport suites purely to move a coverage number
- inventing a synthetic single-number “security score”

## Motivation

The repo already has multiple valuable test signals, but they answer different
questions:

- `./scripts/coverage` answers “what local `s3-tests` integration coverage did
  we hit?”
- `s3-tests` answers “do we still match AWS behavior for these scenarios?”
- `s3-diff-tests` answers “do AWS and the embedded local server produce the
  same response shape for the same request?”
- `s3-http-tests` answers “does the HTTP-only transport path behave correctly?”
- `cargo-fuzz` answers “can malformed attacker input trigger panics or other
  parser failures?”

Those are all useful, but they do not add up to a coherent local security
signal.

Recent security regressions also show why line coverage is the wrong primary
metric for this work:

- authz bugs are often state-matrix and decision-logic bugs
- bucket-policy regressions are semantic drift bugs
- multipart, reclaim, and stream-session failures are race and lifetime bugs
- parser issues are robustness bugs where corpus quality matters more than a
  percentage

The project needs a dedicated answer to a different question:

“Which threat-model surfaces and past security findings are guarded by local,
deterministic tests today, and where are the remaining gaps?”

## Current State

- `./scripts/coverage` currently runs:
  - `cargo llvm-cov clean --workspace`
  - `cargo +nightly test -p s3-tests`
  - `cargo +nightly llvm-cov report`
- the workspace already has distinct suites for:
  - `s3-tests`
  - `s3-diff-tests`
  - `s3-http-tests`
  - `s3-local-tests`
  - crate-local unit and model/property tests
  - parser fuzzing under `fuzz/`
- security findings live under `security/`
- the threat model is documented in `guides/threat_model.md`
- `scripts/security-tests` now provides the first local security-focused runner
- `guides/security-testing.md` now provides the first checked-in surface matrix
- `guides/security-findings-inventory.md` now provides the checked-in
  per-finding map from:
  - past security finding
  - local deterministic regression coverage
  - explicit invalid/stale status where applicable
- there is still no dedicated report wrapper beyond the sectioned output from
  `scripts/security-tests`

## Goals

- make security testing first-class without overloading the integration
  coverage metric
- provide a local deterministic suite that is high signal for security work
- organize security tests by threat-model surface rather than crate boundary or
  line coverage
- ensure each confirmed security finding is mapped to a durable regression path
- make gaps explicit, so “not yet covered” is visible and reviewable

## Non-Goals

- do not collapse all security tests into one new crate on day one
- do not require AWS access for the default local security suite
- do not make fuzzing or long-running randomized jobs mandatory for every local
  iteration
- do not optimize security work around `llvm-cov` percentages

## Design Principles

### 1. Keep Signals Separate

Different suites should keep answering different questions:

- `./scripts/coverage`: local integration coverage movement
- `s3-tests`: AWS compatibility
- `s3-diff-tests`: AWS-vs-local response-shape drift
- `s3-http-tests`: narrow HTTP transport behavior
- fuzzing: malformed-input robustness
- security suite: threat-model and finding regression coverage

The security suite should complement those signals, not replace them.

### 2. Default to Local and Deterministic

The default security suite should be runnable on a normal local development
machine without AWS credentials.

That means it should primarily orchestrate:

- targeted crate tests
- local model/property tests
- local embedded-server tests
- deterministic race-hook regressions

AWS-backed checks remain important, but they should stay labeled as oracle
coverage rather than becoming the default local security suite.

### 3. Organize by Security Surface, Not by Crate

The security suite should group tests by the surfaces the threat model cares
about, for example:

- request parsing and boundary validation
- SigV4 authentication and presigned requests
- POST policy validation
- authorization, ownership controls, ACLs, and public-access-block
- bucket-policy evaluation and enforcement
- multipart, reclaim, and stream-session lifetime behavior
- object lock, retention, lifecycle, and deletion safety
- transport constraints such as HTTP versus secure transport for `SSE-C`
- observability and secret redaction
- storage integrity and corruption handling

This makes the suite useful to reviewers and to future security work.

### 4. Reuse Existing Tests Before Adding New Homes

Most of the needed coverage already belongs in existing crates:

- `auth` for parser/auth correctness and property tests
- `server-core` for authz/stateful/model tests
- `server-http` for transport and cleanup regressions
- `s3-local-tests` for embedded-server local integration behavior

The first version of the suite should orchestrate those existing tests.

Only add a new crate such as `s3-security-tests` if there is a recurring class
of embedded-server local security tests that does not fit the current layout.

### 5. Report Surfaces and Gaps, Not Just Pass/Fail

The suite needs a checked-in matrix showing, for each surface:

- local deterministic coverage
- AWS-oracle coverage
- fuzz/property/stateful coverage
- linked security findings
- linked implementation/testing plans
- remaining gaps

This should be reviewable in code review and easy to update when new findings
arrive.

## Proposed Structure

### Runner

Add a local orchestrator script:

- `scripts/security-tests`

This should provide named groups rather than a single monolithic command.

Suggested initial groups:

- `fast`
- `authn`
- `authz`
- `parser`
- `stateful`
- `transport`
- `all`

The first implementation should stay simple:

- shell script
- explicit `cargo test` invocations
- no new dependencies

### Matrix

Add a checked-in security testing guide or matrix, for example:

- `guides/security-testing.md`

This file should map each threat-model surface and each security finding to:

- the local deterministic test command(s)
- any AWS-backed oracle test command(s)
- any fuzz/property/stateful coverage
- the active plan if coverage is still being built
- current residual gaps

### Optional Report Wrapper

If the suite becomes large enough, add:

- `scripts/security-report`

This should summarize results by surface, not produce a line-coverage
percentage.

The initial version can be much simpler:

- `scripts/security-tests` prints named sections
- CI captures the command output as an artifact or job log

## Initial Surface Inventory

The first matrix should at minimum include these threat-model-aligned areas:

1. Request parsing and protocol boundary hardening
2. SigV4 header auth, presigned auth, and POST auth
3. Authorization and discovery behavior
4. Bucket policy parsing, evaluation, and enforcement
5. Multipart upload and streamed write lifetime behavior
6. Reclaim queue and payload lease behavior
7. Object Lock, retention, legal hold, lifecycle interactions
8. Transport-security-sensitive behavior such as `SSE-C`
9. Observability and secret redaction
10. Storage integrity and corruption handling

For each area, explicitly identify whether current coverage is:

- local deterministic and already good
- AWS-backed only
- fuzz/property/stateful only
- partially covered with an active plan
- mostly uncovered

## Phase 1: Inventory and Matrix

Status: completed.

Deliver:

- add `plans/completed/security-test-suite-plan.md` and then implement the matrix in a
  checked-in guide
- map each threat-model surface from `guides/threat_model.md` to existing test
  locations and commands
- map each security finding under `security/` to:
  - a durable regression test, or
  - an active plan, or
  - an explicit uncovered gap

Success criteria:

- reviewers can answer “what protects this surface locally?” without searching
  across the repo by hand
- every finding in `security/` is visibly tied to a regression path or a known
  gap

## Phase 2: Local Security Runner

Status: completed.

Deliver:

- add `scripts/security-tests`
- implement named groups for the highest-value local deterministic surfaces
- include only local deterministic commands in the default groups

Good initial command candidates:

- targeted `auth` tests for SigV4, presigned, POST policy, and parser work
- targeted `server-core` authz model and regression tests
- targeted `server-core` stateful/race tests as they land
- targeted `server-http` cleanup and transport regressions
- `cargo test -p s3-local-tests`

Do not include by default:

- `s3-diff-tests`
- full AWS-backed `s3-tests`
- long-running fuzzing sessions

Success criteria:

- there is a single local command for “run the security-focused local suite”
- the default run is deterministic and practical for normal development

## Phase 3: Fill the Known Local Gaps

Use the security suite to absorb outputs from completed and ongoing
security-focused work rather than inventing a disconnected new body of tests.

Status: completed.

Priority gaps to wire in:

- authz model coverage from
  `plans/completed/authz-model-testing-plan.md`
- bucket-policy local generator/differential coverage from
  `plans/completed/bucket-policy-differential-testing-plan.md`
- multipart/reclaim/session stateful coverage from
  `plans/completed/multipart-reclaim-stream-session-testing-plan.md`
- parser hardening regressions and fuzz-corpus promotions from
  `plans/completed/parser-hardening-plan.md`

Those areas are no longer blocked on their original plans finishing. The work
here is to inventory the tests they already added and group them into a single
local security-oriented entry point and matrix.

This phase is where the security suite becomes useful rather than just tidy.

Success criteria:

- the main currently-known security testing gaps have a local home in the
  security suite
- new security work automatically updates the suite rather than living only in
  one crate’s ad hoc tests

## Phase 4: Optional Reporting Follow-On

Deliver:

- a small wrapper such as `scripts/security-report` if sectioned
  `scripts/security-tests` output stops being sufficient
- per-surface reporting or sectioned logs so failures are attributable to a
  security area, not just a raw crate test command

Do not turn this into a percentage target.

Success criteria:

- the project keeps a visible local security signal separate from coverage
- failures remain attributable to a threat-model surface

## Validation

When implemented, validation should include:

1. `scripts/security-tests fast`
2. `scripts/security-tests all`
3. targeted verification that each command in the suite is deterministic and
   local
4. review of the matrix against `guides/threat_model.md` and `security/`

## Success Criteria

- the repo has a dedicated local security-focused suite
- the suite is organized by threat-model surface, not by line coverage
- every confirmed security finding maps to a regression path, an active plan,
  or an explicit gap
- `./scripts/coverage` remains focused on local integration coverage instead of
  becoming the only security signal
- AWS, diff, and fuzz jobs remain separate and clearly labeled for the
  questions they answer when they are run

## Recommended Order

1. build the matrix
2. add the local runner
3. wire in the active security-focused plans
4. add a report wrapper later only if sectioned runner output stops being enough
