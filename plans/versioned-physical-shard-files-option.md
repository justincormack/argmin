# Versioned Physical Shard Files Option

Status: option to evaluate, not an accepted design.

Current shard repair keeps the existing logical shard path model. A logical
`ShardKey` maps to the physical shard file path, normal `ShardWrite` keeps its
no-overwrite/idempotent retry behavior, and repair-specific write plumbing is
the only path that may replace the damaged file at that logical path.

An alternative storage layout would make physical shard files generation
addressed. Metadata would publish which physical generation currently satisfies
the logical shard key. Repair could then write a new generation, publish it
through metadata, and leave the old corrupt generation available for reclaim,
audit, or quarantine.

Potential benefits:

- Avoids replacing corrupt evidence in place during repair.
- Gives repair a clearer write-new/publish/reclaim lifecycle.
- May improve crash semantics by separating new data creation from logical
  publication.
- Leaves room for forensic or operational tooling around damaged shard files.

Costs and risks:

- Requires schema changes for logical-to-physical shard generation mapping.
- Requires read-path changes so reads resolve the current generation.
- Requires scavenger, reclaim, and cleanup invariants for old generations.
- Needs careful handling of open read handles and concurrent repair/publish
  races.
- Adds metadata and file churn for the common repair path.

Evaluate this if logical-path replacement creates correctness, audit, recovery,
or operational problems. Until then, the current repair-specific replacement
path remains the simpler model.
