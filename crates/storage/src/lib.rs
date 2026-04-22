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
pub mod error;
pub mod node;
pub mod pg_store;
pub mod schema;
pub mod traits;
pub mod types;

pub use error::{BucketSnapshotLoadError, MetadataError, StoreError};
pub use node::{BucketPairPgGuards, LocalStorageNode, ReclaimWorkItem, SharedStorageNode};
pub use pg_store::PgStore;
pub use s3_types::lifecycle::*;
pub use traits::{PgMetadataStore, ShardStore, StorageNode};
pub use types::*;

#[cfg(test)]
mod tests;
