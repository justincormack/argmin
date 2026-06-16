use storage::ObjectKey;

use super::{
    optional_list_object_key, AuthorizedListObjectVersions, AuthorizedListObjectsV2, Coordinator,
    ListEntry, ListObjectVersionsRequest, ListObjectVersionsResult, ListObjectsResult,
    ListObjectsV2Request, VersionEntry, MAX_LIST_RECORDS, S3_MAX_LIST_KEYS, TRACE_TARGET,
};
use crate::error::ServerError;

impl Coordinator {
    pub fn list_objects_v2(
        &self,
        req: &ListObjectsV2Request,
    ) -> Result<ListObjectsResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::list_objects_v2",
            "bucket={:?} max_keys={}",
            req.bucket.name(),
            req.max_keys
        );
        let bucket = req.bucket.name_typed();
        let prefix = req.prefix;
        let delimiter = req.delimiter;
        let continuation_token = req.continuation_token;
        let max_keys = req.max_keys.min(S3_MAX_LIST_KEYS);
        let AuthorizedListObjectsV2 { bucket_info } = self.authorize_list_objects_v2(req)?;
        let owner_principal = bucket_info.owner_principal.clone();
        let owner_canonical_id = bucket_info.owner_canonical_id.clone();

        if max_keys == 0 {
            return Ok(ListObjectsResult {
                objects: Vec::new(),
                common_prefixes: Vec::new(),
                is_truncated: false,
                next_continuation_token: None,
                owner_principal,
                owner_canonical_id,
            });
        }

        let list_prefix = optional_list_object_key(prefix)?;
        let list_start_after = optional_list_object_key(continuation_token)?;

        let listed = self
            .storage_node()
            .list_objects_for_bucket(
                bucket,
                list_prefix.as_ref(),
                delimiter,
                list_start_after.as_ref(),
                MAX_LIST_RECORDS,
                max_keys,
            )
            .map_err(Self::map_object_pg_action_error)?;

        let mut objects: Vec<ListEntry> = Vec::new();
        for obj in &listed.objects {
            let obj_key = obj.key();
            let record = obj
                .as_live()
                .expect("list_objects returns only live objects");
            let system_metadata = self.deserialize_visible_system_metadata(
                record.system_metadata_blob.as_ref(),
                &record.encryption,
                None,
            )?;
            objects.push(ListEntry {
                key: obj_key.to_string(),
                size: record.size,
                etag: record.etag.format(),
                last_modified: record.last_modified,
                checksum_algorithm: system_metadata.checksum_algorithm(),
                checksum_type: system_metadata.checksum_type(),
            });
        }
        let common_prefixes = listed
            .common_prefixes
            .into_iter()
            .map(|prefix| prefix.to_string())
            .collect();

        Ok(ListObjectsResult {
            objects,
            common_prefixes,
            is_truncated: listed.is_truncated,
            next_continuation_token: listed
                .next_continuation_token
                .map(|token| token.to_string()),
            owner_principal,
            owner_canonical_id,
        })
    }

    pub fn list_object_versions(
        &self,
        req: &ListObjectVersionsRequest,
    ) -> Result<ListObjectVersionsResult, ServerError> {
        let max_keys = req.max_keys.min(S3_MAX_LIST_KEYS);
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::list_object_versions",
            "bucket={:?} max_keys={}",
            req.bucket.name(),
            max_keys
        );
        let bucket = req.bucket.name_typed();
        let prefix = req.prefix;
        let delimiter = req.delimiter;
        let key_marker = req.key_marker;
        let version_id_marker = req.version_id_marker;
        let AuthorizedListObjectVersions { bucket_info } =
            self.authorize_list_object_versions(req)?;
        let owner_principal = bucket_info.owner_principal.clone();
        let owner_canonical_id = bucket_info.owner_canonical_id.clone();

        if max_keys == 0 {
            return Ok(ListObjectVersionsResult {
                versions: Vec::new(),
                common_prefixes: Vec::new(),
                is_truncated: false,
                next_key_marker: None,
                next_version_id_marker: None,
                owner_principal,
                owner_canonical_id,
            });
        }

        let list_prefix = optional_list_object_key(prefix)?;
        let list_key_marker = optional_list_object_key(key_marker)?;
        let listed = self
            .storage_node()
            .list_object_versions_for_bucket(
                bucket,
                list_prefix.as_ref(),
                delimiter,
                list_key_marker.as_ref(),
                version_id_marker,
                max_keys,
            )
            .map_err(Self::map_object_pg_action_error)?;

        let mut versions: Vec<VersionEntry> = Vec::new();
        let mut last_key: Option<&ObjectKey> = None;

        for obj in &listed.versions {
            let obj_key = obj.key();
            let is_latest = last_key.is_none_or(|k| k != obj_key);
            if is_latest {
                last_key = Some(obj_key);
            }

            let (size, etag, checksum_algorithm, checksum_type) = match obj.as_live() {
                Some(record) => {
                    let system_metadata = self.deserialize_visible_system_metadata(
                        record.system_metadata_blob.as_ref(),
                        &record.encryption,
                        None,
                    )?;
                    (
                        record.size,
                        record.etag.format(),
                        system_metadata.checksum_algorithm(),
                        system_metadata.checksum_type(),
                    )
                }
                None => (0, String::new(), None, None),
            };

            versions.push(VersionEntry {
                key: obj_key.to_string(),
                version_id: obj.version_id(),
                is_latest,
                size,
                etag,
                last_modified: obj.last_modified(),
                is_delete_marker: obj.is_delete_marker(),
                checksum_algorithm,
                checksum_type,
            });
        }

        Ok(ListObjectVersionsResult {
            versions,
            common_prefixes: listed
                .common_prefixes
                .into_iter()
                .map(|prefix| prefix.to_string())
                .collect(),
            is_truncated: listed.is_truncated,
            next_key_marker: listed.next_key_marker.map(|key| key.to_string()),
            next_version_id_marker: listed.next_version_id_marker,
            owner_principal,
            owner_canonical_id,
        })
    }
}
