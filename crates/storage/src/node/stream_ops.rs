use super::*;

impl SharedStorageNode {
    pub fn create_put_object_stream_session<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        request: BucketSnapshotRequest,
        action: impl FnOnce(
            BucketSnapshot,
            Option<StoredObject>,
        ) -> Result<(T, CreateStreamUploadReq), E>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        loop {
            let pg_id = self.pg_topology.bucket_pg_for(bucket);
            let bucket_pg = self.get_pg(pg_id)?;
            match PgMetadataStore::acquire_bucket_write_reservation(&*bucket_pg, bucket) {
                Ok(_info) => {
                    let snapshot = Self::load_bucket_snapshot_from_pg(&bucket_pg, bucket, request)?;
                    drop(bucket_pg);

                    let object_pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
                    let existing_object =
                        Self::load_existing_live_object_from_object_pg(&object_pg, bucket, key)?;
                    let result = match action(snapshot, existing_object) {
                        Ok((value, create)) => {
                            object_pg.create_stream_upload(&create)?;
                            Ok(value)
                        }
                        Err(error) => Err(error),
                    };
                    drop(object_pg);

                    let release_result = self.release_bucket_write_reservation(bucket);
                    return Self::finish_bucket_write_snapshot(result, release_result);
                }
                Err(crate::error::MetadataError::BucketWriteDraining) => {
                    drop(bucket_pg);
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                Err(other) => return Err(other.into()),
            }
        }
    }
}
