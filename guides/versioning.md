<!-- Copyright The Argmin Authors. -->
<!-- SPDX-License-Identifier: CC-BY-4.0 -->

# Versioned Formats And Compatibility

This guide defines how Argmin owns and changes durable and cross-process
formats. The current format inventory, evidence status, and implementation
work are tracked in
[`storage-upgrade-versioning-plan.md`](../plans/storage-upgrade-versioning-plan.md).
The plan is allowed to change as work progresses; this guide states the
engineering rules that should remain true.

This is internal format versioning. It is separate from Rust API stability,
crate versions, and the S3 protocol exposed to clients.

## Current Pre-Release Policy

Argmin does not currently support upgrading an existing deployment or running
a cluster containing different format versions. There are no supported legacy
formats.

Consequently:

- current code writes and reads only the current format;
- missing, malformed, older, and newer versions fail closed;
- old readers, migrations, fallback decoding, inferred versions, and
  compatibility defaults must not be added;
- nullable fields or alternate representations must not be retained for a
  hypothetical upgrade; and
- a fresh deployment must be initialized with the current binary.

Exact fixtures for an older version are evidence, not compatibility support.
Keeping its bytes in a test does not authorize production code to decode it.

When real deployments require upgrades, we will add an explicit upgrade
framework and compatibility guides. Those guides will describe the supported
source and destination versions, mixed-version rules, rollout and rollback
constraints, and recovery procedures. Until then, an unsupported version is a
hard boundary rather than an invitation to recover or reinterpret state.

## What Must Be Versioned

A version boundary is any representation that may be persisted, authenticated,
hashed, replayed, or exchanged independently. This includes:

- database schemas and complete physical catalogues;
- directory layouts, identity files, journals, WALs, restart artifacts, and
  sentinels;
- RPC, transport, authentication, and request/response frames;
- command, checkpoint, snapshot, canonical text, and configuration formats;
- values nested inside rows or other formats when their encoding can change
  independently; and
- digest inputs, domain separators, hash chains, cryptographic profiles, and
  opaque carrier encodings.

A format does not become safe merely because it is nested inside another
versioned format. Either the nested value has its own version, or the format
dependency ledger must say which containing versions advance whenever it
changes incompatibly.

## Ownership And Containment

Every format has exactly one owning crate. The owner keeps its byte layout,
encoder, decoder, validation, and future migration code private or
crate-private. For an independently versioned or self-describing format, this
also includes its marker and version constant. A deliberately untagged format
instead remains governed by its owner-controlled encoding and the explicit
dependency ledger binding incompatible changes to its containing versions.
Other crates receive typed logical values, capabilities, semantic failures,
and opaque carriers. They must not construct or interpret the owner's raw
bytes, wire tags, SQL, filenames, or backend errors.

Malformed-format and impossible-state tests belong with the owner. Higher
layers test logical behavior or use a narrow, opaque test facility supplied by
the owner. Rust visibility and crate dependencies are the primary enforcement;
repository boundary checks supplement them where a representation is easy to
leak accidentally.

## Reader And Writer Rules

For an independently versioned or self-describing format, the current writer
always emits the current marker and version. Its authoritative reader must
distinguish, where applicable:

- missing or truncated framing;
- unknown magic;
- malformed or noncanonical version syntax;
- unsupported older or newer versions;
- integrity or authentication failure; and
- invalid current-version contents.

A deliberately untagged format has no marker to emit or reject. Its owner must
still define and seal the exact encoding. The dependency ledger must name every
containing version that advances when that encoding changes incompatibly.

Version rejection happens before the rejected content can cause dispatch,
mutation, replay, repair, or publication. It also happens before format-owned
unbounded allocation. An outer transport may necessarily read a bounded frame
before discovering a nested marker; that does not permit nested allocation or
dispatch before the nested version is accepted.

Authenticated and checksummed rejection fixtures must be correctly resealed.
Otherwise a test may exercise only the integrity check and provide no evidence
that the version boundary works.

## Evidence Is Part Of The Format

Every version, and every deliberately untagged owner-controlled encoding, has
immutable, owner-local representation evidence. Depending on the format, this
is an exact byte fixture, complete physical schema manifest, canonical text
fixture, registry fingerprint, digest vector, or cryptographic vector.

Evidence must be complete enough that a coordinated encoder and decoder change
cannot silently redefine the same version. Variant formats therefore need
compiler-exhaustive construction and exact comparison with an authoritative
production registry. Optional fields need both arms for each distinct field,
not merely one `None` and one `Some` somewhere in an aggregate.

Evidence is append-only and indexed by version:

- an independently versioned format retains immutable evidence keyed by its
  own version;
- an outer grammar with an independently versioned opaque payload retains
  immutable outer evidence keyed by the outer version; and
- a composite also retains fixtures keyed by the complete relevant version
  vector, such as `(snapshot_v1, state_v28)`.

If only an independently versioned nested format changes and the containing
field's grammar is unchanged, advance the nested version and append a new
composite-vector fixture. Do not advance the outer version. If field width,
order, optionality, framing, or interpretation changes, advance the outer
version too. An incompatible change to a deliberately untagged nested encoding
instead advances every containing version named by its dependency ledger.

Old and new rejection fixtures remain after an advancement. The previous
version's evidence must never be rewritten to match the new implementation.

## Commit Boundaries Are Evidence Boundaries

Do not bump the same format more than once within a commit. Every emitted
format version must exist at a repository commit boundary where its writer,
reader, fixtures, and dependency rules can be built, tested, and independently
reviewed.

The version being replaced must already have complete evidence in the parent
commit. If that evidence is missing, first make and validate an evidence-only
commit against the unchanged old writer. Only a later commit may change the
format and advance its version. Adding a reconstruction of the old format in
the same commit that removes it is not equivalent: there is no commit boundary
at which the evidence can be checked against the production writer it claims
to describe.

For example, a commit must not introduce version 4 and then change it to
version 5 before the commit is made. There is no repository state against which
version 4's purported evidence can be validated, so version 4 was never a
meaningful format version. If further incompatible work is discovered before
commit, either:

- fold it into the same pending version and retain one transition; or
- split the work into sequential commits, each of which has a complete,
  passing implementation and immutable evidence for the version it introduces.

Several directly dependent formats may advance together in one atomic commit.
The rule is one observable transition per format, not one changed format per
commit. A coordinated version vector must be complete at that boundary.

This also means a version bump must not be committed without its evidence, and
evidence must not claim a version that no commit actually wrote.

## Changing A Format

Before making an incompatible change:

1. Identify the owner, current exact encoding, authoritative writer and reader,
   dependency ledger, and all direct and indirect containing formats. Also
   identify the marker and version where the format is independently versioned
   or self-describing.
2. Confirm that the parent commit already contains exact evidence for the
   current encoding. For a deliberately untagged encoding, that evidence must
   include every affected containing-version vector named by its dependency
   ledger. If the evidence is missing, add and commit it without changing the
   format, then begin the incompatible change. Never regenerate old evidence
   with the new writer.
3. Decide which versions advance. An incompatible change to a deliberately
   untagged nested encoding advances every containing version named by its
   dependency ledger.
4. Change the owner-controlled encoding and every affected containing writer
   and rejecting reader atomically. Do not add a fallback reader.
5. Add immutable evidence for the new encoding and append complete
   version-vector fixtures for composites.
6. For every advanced version, add correctly sealed older/newer rejection cases
   at owner and production admission boundaries, proving no mutation, replay,
   repair, or publication.
7. Update the format inventory, dependency rules, and code comments in the
   same slice.
8. Verify the complete slice at a commit boundary before beginning another
   transition for that format.

A compatible change still requires care. If it changes canonical bytes,
accepted grammar, semantic interpretation, digest inputs, or durable
invariants, it is normally an incompatible format change even when the Rust
types still compile.

## Future Upgrade And Compatibility Work

Upgrade support will be designed deliberately rather than emerging as a set of
fallback parsers. At minimum, it will require:

- an explicit supported-version and compatibility matrix;
- ordered, crash-safe upgrade steps with preconditions, postconditions, and
  verification scans;
- a defined rule for offline versus rolling upgrades and mixed-version
  clusters;
- protocol negotiation and feature gates where mixed versions are supported;
- durable progress records and restart behavior for interrupted upgrades;
- failure behavior that leaves state closed and diagnosable; and
- version-specific operator compatibility and upgrade guides.

Normal crash recovery must not double as an implicit migration. Likewise,
repair code must not reinterpret an unsupported format. Compatibility code and
its removal policy will be part of the upgrade design when that work begins.
