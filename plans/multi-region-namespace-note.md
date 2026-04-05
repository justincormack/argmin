# Multi-Region Namespace Note

This is a short future-work note, not an implementation plan.

## Current position

Argmin currently emulates a single S3 region.

That is fine for the current roadmap. We do not plan to implement AWS-style
multi-region bucket placement now.

## Important compatibility point

AWS S3 bucket naming is not region-local.

Within an AWS partition, the bucket namespace is global:

- standard AWS regions share one bucket namespace
- standard general-purpose buckets still require global coordination
- GovCloud is a separate partition with its own namespace
- other partitions should be treated similarly

AWS has also added an account-regional namespace for some general-purpose
bucket names. Buckets named with the `-<account-id>-<region>-an` suffix are
locked to that owning account and region. That means this specific naming form
can be validated locally for owner/region correctness, and it does encode the
bucket's region directly.

That does not remove the broader compatibility requirement for the normal
global namespace. It is a scoped exception, not a replacement model.

That means a true multi-region S3-compatible deployment cannot be modeled as
fully independent regional servers that know only their own local buckets.

They need some shared control-plane behavior for at least:

- global bucket name allocation within the partition
- mapping bucket name to owning region
- returning the correct region hint for wrong-region requests
- preventing duplicate bucket creation in different regions
- handling account-regional bucket names as a special locally-validatable case

## Why this matters

If we ever add multi-region support, we should not accidentally design it as
"one isolated server per region" with no shared namespace coordination.

That would diverge from AWS in a fundamental way:

- bucket creation would be incorrectly region-local
- bucket existence checks would be incomplete
- wrong-region redirects and `x-amz-bucket-region` behavior would not be
  authoritative unless bucket placement is globally known

## If revisited later

Treat multi-region support as a control-plane feature first, not just a
data-plane replication or routing feature.

A future design should start by defining:

- partition boundaries
- bucket namespace ownership and coordination
- bucket-to-region metadata authority
- how regional frontends learn and cache that mapping

Until then, the compatibility target should remain explicit:

- Argmin behaves as a single-region S3 implementation
