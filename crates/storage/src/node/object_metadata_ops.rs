// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
#[cfg(test)]
use s3_types::{AclGrants, LegalHoldStatus, ObjectRetention, StoredLegalHoldStatus};

impl SharedStorageNode {
    #[cfg(test)]
    fn with_object_metadata_if<T>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        action: impl FnOnce(&PgStore, &StoredObject) -> Result<T, ObjectPgActionError>,
    ) -> Result<T, ObjectPgActionError> {
        let pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        let stored = match version_id {
            Some(version_id) => PgMetadataStore::get_object_version(&*pg, bucket, key, version_id)?,
            None => PgMetadataStore::get_object_meta(&*pg, bucket, key)?,
        };
        action(&pg, &stored)
    }

    #[cfg(test)]
    pub fn put_object_tags_if<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        tags: &crate::SerializedTagSet,
        action: impl FnOnce(&StoredObject) -> Result<VersionId, E>,
    ) -> Result<Result<VersionId, E>, ObjectPgActionError> {
        self.with_object_metadata_if(bucket, key, version_id, |pg, stored| match action(stored) {
            Ok(version_id) => {
                PgMetadataStore::put_object_tags(pg, bucket, key, version_id, tags)?;
                Ok(Ok(version_id))
            }
            Err(error) => Ok(Err(error)),
        })
    }

    #[cfg(test)]
    pub fn delete_object_tags_if<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        action: impl FnOnce(&StoredObject) -> Result<VersionId, E>,
    ) -> Result<Result<(), E>, ObjectPgActionError> {
        self.with_object_metadata_if(bucket, key, version_id, |pg, stored| match action(stored) {
            Ok(version_id) => {
                PgMetadataStore::delete_object_tags(pg, bucket, key, version_id)?;
                Ok(Ok(()))
            }
            Err(error) => Ok(Err(error)),
        })
    }

    #[cfg(test)]
    pub fn put_object_retention_if<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        retention: ObjectRetention,
        action: impl FnOnce(&StoredObject) -> Result<VersionId, E>,
    ) -> Result<Result<VersionId, E>, ObjectPgActionError> {
        self.with_object_metadata_if(bucket, key, version_id, |pg, stored| match action(stored) {
            Ok(version_id) => {
                PgMetadataStore::put_object_retention(pg, bucket, key, version_id, retention)?;
                Ok(Ok(version_id))
            }
            Err(error) => Ok(Err(error)),
        })
    }

    #[cfg(test)]
    pub fn put_object_legal_hold_if<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        legal_hold: StoredLegalHoldStatus,
        action: impl FnOnce(&StoredObject) -> Result<VersionId, E>,
    ) -> Result<Result<VersionId, E>, ObjectPgActionError> {
        self.with_object_metadata_if(bucket, key, version_id, |pg, stored| match action(stored) {
            Ok(version_id) => {
                PgMetadataStore::put_object_legal_hold(pg, bucket, key, version_id, legal_hold)?;
                Ok(Ok(version_id))
            }
            Err(error) => Ok(Err(error)),
        })
    }

    #[cfg(test)]
    pub fn put_object_acl_if<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        action: impl FnOnce(&StoredObject) -> Result<(VersionId, AclGrants, bool), E>,
    ) -> Result<Result<VersionId, E>, ObjectPgActionError> {
        self.with_object_metadata_if(bucket, key, version_id, |pg, stored| match action(stored) {
            Ok((version_id, acl_grants, public_read)) => {
                PgMetadataStore::put_object_acl(
                    pg,
                    bucket,
                    key,
                    version_id,
                    &acl_grants,
                    public_read,
                )?;
                Ok(Ok(version_id))
            }
            Err(error) => Ok(Err(error)),
        })
    }

    #[cfg(test)]
    pub fn get_object_legal_hold_if<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        action: impl FnOnce(&StoredObject) -> Result<Option<LegalHoldStatus>, E>,
    ) -> Result<Result<Option<LegalHoldStatus>, E>, ObjectPgActionError> {
        self.with_object_metadata_if(bucket, key, version_id, |_pg, stored| {
            match action(stored) {
                Ok(legal_hold) => Ok(Ok(legal_hold)),
                Err(error) => Ok(Err(error)),
            }
        })
    }

    #[cfg(test)]
    pub fn get_object_retention_if<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        action: impl FnOnce(&StoredObject) -> Result<Option<ObjectRetention>, E>,
    ) -> Result<Result<Option<ObjectRetention>, E>, ObjectPgActionError> {
        self.with_object_metadata_if(bucket, key, version_id, |_pg, stored| {
            match action(stored) {
                Ok(retention) => Ok(Ok(retention)),
                Err(error) => Ok(Err(error)),
            }
        })
    }
}
