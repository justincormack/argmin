# Unified Segment Payload Redesign

## Motivation

The current codebase has two different physical payload models:

1. **Direct shard-set payloads**
   - normal `PutObject`
   - non-streamed multipart parts
   - integrity boundary is effectively the whole shard file

2. **Chunk-manifest payloads**
   - streamed `PutObject`
   - streamed multipart parts
   - integrity boundary is one committed chunk

This split was useful to get bounded-memory streaming writes landed, but it is not
the right long-term model.

It conflates:

- transfer/ingest chunking for slow writers
- physical storage layout
- read integrity granularity

The result is:

- different correctness and performance properties depending on how the object was
  uploaded
- read-path optimization is harder because the storage model is inconsistent
- small range reads are naturally safe on chunk-manifest objects but not on direct
  shard-set objects
- slow-writer handling does not apply uniformly to normal `PUT` and multipart part
  uploads

This redesign proposes one fixed-segment physical payload model for all object data.

## Goal

Adopt a single internal payload representation:

- every object payload is stored as a sequence of fixed-size internal segments
- every segment is stored as one shard set
- every segment has its own integrity checksum
- transfer style (`PUT`, aws-chunked, `POST`, `UploadPart`, `CopyObject`) is
  decoupled from physical segment layout

This should eventually retire the current "direct shard-set vs chunk-manifest"
split as a first-class storage distinction.

## Core Model

### Segment

A segment is the fixed physical storage unit for payload data.

Each segment has:

- segment index
- logical segment size
- object payload generation id
- segment payload generation id (or equivalent immutable identity)
- shard placement (`pg`, `okh`, EC shape)
- integrity checksum

Segments are:

- fixed-size except the tail segment
- independent of client transfer chunking
- the integrity boundary for partial reads

### Payload manifest

An object payload is a manifest of segments.

For a normal object:

- the payload is just an ordered segment list

For multipart:

- the working design is that parts reference ordered segment sublists
- the committed multipart object keeps explicit part structure
- each part then carries an ordered segment list

This preserves multipart semantics directly for:

- `ListParts`
- `CompleteMultipartUpload`
- multipart ETag/composite checksum rules
- per-part reclaim and copy-source behavior

## Design Principles

1. **Transfer style must not determine storage shape**
   - slow streaming writers and fast buffered writers should converge onto the same
     physical model

2. **Integrity boundary must match efficient read granularity**
   - partial reads should be able to validate the touched segments without reading
     whole-object shard files

3. **Memory must stay bounded**
   - readers and writers should work segment-by-segment

4. **Retention and reclaim must work at manifest level**
   - leases and reclaim records should retain segment manifests, not ad hoc special
     cases by upload mode

5. **The model should simplify, not multiply, object layouts**
   - special-case `ChunkManifest` should not remain a permanent separate concept if
     all payloads become segmented internally

## What Changes

### Writes

All object-data writes should accumulate into fixed-size internal segments:

- normal `PUT`
- aws-chunked `PUT`
- `POST Object`
- `UploadPart`
- `CopyObject`
- `UploadPartCopy`

Slow writer behavior becomes:

1. accept incoming bytes incrementally
2. accumulate until one internal segment is full
3. encode/write that segment as a shard set
4. append one manifest row

This solves the original "slow writer ties up the system" problem uniformly, not
only for special streaming paths.

### Reads

All reads become manifest-driven:

- full `GET`
- range `GET`
- `GetObjectPart`
- copy-source reads

Partial reads can then:

1. compute touched segment indices
2. read only those segments
3. validate segment checksums
4. slice the requested range

### Integrity

Per-segment checksums become the read integrity boundary.

The current whole-shard CRC can remain as:

- a coarse integrity check
- scrub signal
- transitional defense while the unified segment format is being rolled out

But efficient partial reads should depend on segment checksums.

## Key Open Design Questions

### 1. Segment size

This is the first major design choice.

Tradeoffs:

- smaller segments:
  - lower read amplification for small ranges
  - more metadata rows
  - more shard files
  - more write-side overhead

- larger segments:
  - better throughput and lower metadata churn
  - worse small-range over-read

Candidate starting points:

- `1 MiB`
- `4 MiB`

This should be chosen deliberately, not inherited from current HTTP chunking.

### 2. Manifest shape for multipart

Working decision:

1. **Part hierarchy**
   - multipart object manifest contains ordered parts
   - each part contains ordered segments
   - this is the default design unless later implementation evidence forces a
     change

We should not flatten multipart into one segment list unless there is a strong
operational reason to do so.

### 3. Identity model

We now have `GenerationId` for payload generations.

Working decision:

- reclaim identity is rooted only at object payload generation
- segments belong to exactly one object generation
- segments from different object generations must never be mixed semantically,
  even if some lower-level storage optimization might one day deduplicate
  physical bytes

This means:

- each object payload generation has one root `GenerationId`
- reclaim records and leases are keyed by that root generation
- segment identities are subordinate payload-location identities, not separate
  reclaim roots

### 4. Metadata schema strategy

Possible directions:

1. Introduce generic segment-manifest tables and migrate toward them.
2. Evolve current `stream_object_chunks` / `multipart_part_chunks` tables into the
   generic segment model.

There are no backward compatibility guarantees at this stage, so we should choose
the cleaner resulting schema rather than optimize for incremental migration.

## Recommended Direction

### Short version

Use one fixed-segment physical payload model for all newly written object data.

Specifically:

1. choose a fixed segment size
2. treat current chunk-manifest work as the prototype for the generic segment
   manifest model
3. stop treating `ChunkManifest` as a special upload-mode layout over time
4. unify integrity, read, reclaim, and slow-writer behavior around segments
5. adopt the new metadata/layout model directly, without carrying compatibility
   baggage for earlier experimental internal layouts

### Why not keep the current split?

Because it leaves us permanently with:

- two storage models
- two integrity models
- two reclaim models
- two read-optimization stories

That is the wrong complexity profile.

## Proposed Implementation Phases

### Phase A: Design final segment model

Decide:

1. fixed segment size
2. multipart manifest shape
3. identity model
4. schema evolution strategy

This document is the starting point for that design.

### Phase B: Add generic segment metadata

Introduce the generic segment-manifest representation in storage.

Goals:

- enough metadata to describe any payload as ordered segments
- segment-level integrity checksum
- reclaim compatibility
- clean replacement of the current experimental layout split

### Phase C: Route new streamed writes through generic segments

Switch current streaming paths to write generic segments rather than a special
chunk-manifest concept.

### Phase D: Route normal `PUT` and non-streamed multipart through generic segments

This is the key convergence step:

- normal `PUT` no longer creates a special direct shard-set payload
- buffered writes are segmented internally just like slow writes

### Phase E: Switch reads and copy-source paths to generic segments

Once all new payloads use segments, read and copy paths should consume a unified
manifest abstraction.

### Phase F: Retire the special-case chunk-manifest model

At that point:

- `ChunkManifest` should no longer be a first-class permanent object-layout split
- it becomes an implementation detail of the generic segment model, or disappears
- older experimental layouts can be removed outright rather than preserved

## Non-Goals

1. Changing user-visible multipart semantics
2. Changing S3 versioning semantics
3. Solving full account/ownership design here
4. Preserving compatibility with earlier experimental internal layouts

## Success Criteria

The redesign is successful if:

1. slow streaming writers and normal buffered writers share the same physical
   payload model
2. small range reads can validate only touched segments
3. reclaim and leases operate on one payload-graph model
4. read-path optimization no longer depends on whether the object was originally
   streamed
5. the number of storage-layout special cases decreases rather than increases
