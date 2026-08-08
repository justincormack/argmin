pub trait StorageClusterObjectTestSupport {
    fn opaque_error(&self) -> Result<(), TestStorageFailure>;
    fn raw_shard_error(&self) -> ShardIoError;
    fn other_fallible_error(
        &self,
        _misleading_parameter: TestStorageFailure,
    ) -> Result<(), OtherError>;
}
