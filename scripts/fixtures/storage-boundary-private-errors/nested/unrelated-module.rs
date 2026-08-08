pub mod error;

pub enum StoreError {}
pub enum MetadataError {}
pub enum ObjectPgActionError {}
pub enum ShardIoError {}

pub use error::{MetadataError, StoreError};

pub enum ClusterBuildError {
    OpenLocalNode { source: StoreError },
}

pub enum StorageNodeServerError {
    Store(StoreError),
}
