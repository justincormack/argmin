/// Storage layer for argmin2.
///
/// Provides per-PG shard I/O with CRC64-NVME integrity checksums,
/// per-PG object metadata in SQLite, and a global bucket table.
///
/// All IO is synchronous. Single-node, single-process for v1-minimal.
pub mod bucket_db;
pub mod error;
pub mod memory_store;
pub mod node;
pub mod pg_store;
pub mod schema;
pub mod traits;
pub mod types;

pub use bucket_db::SqliteBucketDb;
pub use error::{MetadataError, StoreError};
pub use memory_store::MemoryPgStore;
pub use node::{LocalStorageNode, SharedStorageNode};
pub use pg_store::PgStore;
pub use traits::{GlobalService, PgMetadataStore, ShardStore, StorageNode};
pub use types::*;

#[cfg(test)]
pub mod test_util;

#[cfg(test)]
mod tests;
