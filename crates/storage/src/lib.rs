#![allow(
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::doc_markdown,
    clippy::items_after_statements,
    clippy::manual_let_else,
    clippy::match_same_arms,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    clippy::needless_pass_by_value,
    clippy::redundant_closure_for_method_calls,
    clippy::similar_names,
    clippy::too_many_lines,
    clippy::uninlined_format_args,
    clippy::unreadable_literal,
    clippy::wildcard_imports
)]

/// Storage layer for argmin2.
///
/// Provides per-PG shard I/O with CRC64-NVME integrity checksums,
/// and per-PG object/bucket metadata in SQLite.
///
/// All IO is synchronous. Single-node, single-process for v1-minimal.
pub mod clock;
pub mod cluster;
pub mod error;
pub(crate) mod metadata_command;
pub mod node;
pub mod pg_store;
pub mod pg_topology;
pub mod schema;
pub mod shard_key_hash;
pub mod traits;
pub mod types;

pub use cluster::{
    BucketWriteSnapshotAction, LocalClusterMap, LocalNodeStore, LocalNodeStoreConfig, LocalPgRoute,
    ObjectPayloadLease, ReleasedObjectPayloadLease, ShardLocation, StorageCluster,
};
#[cfg(feature = "test-hooks")]
pub use cluster::{
    MetadataCommandApplyContextTestHook, MetadataCommandApplyContextTestHookGuard,
    MetadataCommandApplyTestContext, MetadataCommandApplyTestKind,
};
pub use error::{
    BucketSnapshotLoadError, BucketWriteDrainError, ClusterBuildError, MetadataError,
    ObjectPgActionError, ShardIoError, StoreError,
};
pub use metadata_command::BucketWriteReservationProof;
#[cfg(feature = "test-hooks")]
pub use node::{
    install_bucket_scoped_test_hooks, BucketScopedTestHookGuard, BucketScopedTestHooks,
};
pub use node::{
    BucketCreateAttemptOutcome, BucketDeleteFinalizeOutcome, BucketPairPgGuards,
    BucketWriteDrainGuard, LocalStorageNode, ReclaimWorkItem, SharedStorageNode,
};
pub use pg_store::{MetadataCommandLogCompactionStatus, MetadataCommandLogStats, PgStore};
pub use pg_topology::PgTopology;
pub use placement::NodeId;
pub use s3_types::lifecycle::*;
pub use shard_key_hash::{
    multipart_part_segment_key_hash, object_key_hash, part_key_hash, segment_key_hash,
    stream_segment_key_hash,
};
#[cfg(test)]
pub(crate) use traits::PgMetadataStore;
pub use traits::{ShardStore, StorageNode};
pub use types::*;

#[cfg(test)]
mod tests;
