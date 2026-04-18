use storage::{ListObjectVersionsReq, ListObjectsReq, ObjectKey, PgMetadataStore, StoredObject};

use super::{
    optional_list_object_key, parse_list_object_key, AuthorizedListObjectVersions,
    AuthorizedListObjectsV2, Coordinator, ListEntry, ListObjectVersionsRequest,
    ListObjectVersionsResult, ListObjectsResult, ListObjectsV2Request, VersionEntry,
    MAX_LIST_RECORDS, S3_MAX_LIST_KEYS, TRACE_TARGET,
};
use crate::error::ServerError;

impl Coordinator {
    pub fn list_objects_v2(
        &self,
        req: &ListObjectsV2Request,
    ) -> Result<ListObjectsResult, ServerError> {
        #[derive(Clone)]
        enum ListObjectsPageStart {
            After(ObjectKey),
            At(ObjectKey),
        }

        struct ObjectCursor {
            pg_id: u32,
            objects: Vec<StoredObject>,
            next_index: usize,
            next_page_start: Option<ListObjectsPageStart>,
        }

        impl ObjectCursor {
            fn current(&self) -> Option<&StoredObject> {
                self.objects.get(self.next_index)
            }
        }

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

        let fetch_limit = max_keys.saturating_add(1);
        let list_prefix = optional_list_object_key(prefix)?;
        let list_start_after = optional_list_object_key(continuation_token)?;

        if delimiter.is_none() {
            let mut all_objects: Vec<StoredObject> = Vec::new();
            let mut hit_record_cap = false;
            self.pg_topology.for_each_pg(|pg_id| {
                if hit_record_cap {
                    return Ok::<(), ServerError>(());
                }
                let pg = self.storage_node.get_pg(pg_id)?;
                let resp = pg.list_objects(&ListObjectsReq {
                    bucket: bucket.clone(),
                    prefix: list_prefix.clone(),
                    start_after: list_start_after.clone(),
                    start_at: None,
                    max_keys: fetch_limit,
                })?;
                all_objects.extend(resp.objects);
                if all_objects.len() >= MAX_LIST_RECORDS {
                    all_objects.truncate(MAX_LIST_RECORDS);
                    hit_record_cap = true;
                }
                Ok::<(), ServerError>(())
            })?;

            all_objects.sort_by(|a, b| a.key().cmp(b.key()));
            all_objects.dedup_by(|a, b| a.key() == b.key());

            let max = max_keys as usize;
            let mut objects: Vec<ListEntry> = Vec::new();
            let mut last_entry: Option<String> = None;
            for obj in &all_objects {
                if objects.len() >= max {
                    break;
                }
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
                last_entry = Some(obj_key.to_string());
            }

            let is_truncated = hit_record_cap || all_objects.len() > max;
            let next_token = if is_truncated { last_entry } else { None };

            return Ok(ListObjectsResult {
                objects,
                common_prefixes: Vec::new(),
                is_truncated,
                next_continuation_token: next_token,
                owner_principal,
                owner_canonical_id,
            });
        }

        let prefix_str = prefix.unwrap_or("");
        let delimiter = delimiter.expect("checked above");
        let initial_start = match continuation_token.filter(|token| !token.is_empty()) {
            Some(token) => {
                let token = parse_list_object_key(token)?;
                let token_str = token.as_str();
                if let Some(after_prefix) = token_str.strip_prefix(prefix_str) {
                    if after_prefix.ends_with(delimiter) {
                        if let Some(upper_bound) = storage::object_key_prefix_upper_bound(&token) {
                            Some(ListObjectsPageStart::At(upper_bound))
                        } else {
                            Some(ListObjectsPageStart::After(token))
                        }
                    } else {
                        Some(ListObjectsPageStart::After(token))
                    }
                } else {
                    Some(ListObjectsPageStart::After(token))
                }
            }
            None => None,
        };

        let fetch_objects_page = |cursor: &mut ObjectCursor,
                                  start: Option<ListObjectsPageStart>|
         -> Result<(), ServerError> {
            let (start_after, start_at) = match start {
                Some(ListObjectsPageStart::After(key)) => (Some(key), None),
                Some(ListObjectsPageStart::At(key)) => (None, Some(key)),
                None => (None, None),
            };
            let pg = self.storage_node.get_pg(cursor.pg_id)?;
            let resp = pg.list_objects(&ListObjectsReq {
                bucket: bucket.clone(),
                prefix: list_prefix.clone(),
                start_after,
                start_at,
                max_keys: fetch_limit,
            })?;
            cursor.objects = resp.objects;
            cursor.next_index = 0;
            cursor.next_page_start = resp.next_start_after.map(ListObjectsPageStart::After);
            Ok(())
        };

        let refill_cursor = |cursor: &mut ObjectCursor| -> Result<(), ServerError> {
            while cursor.current().is_none() {
                let Some(next_start) = cursor.next_page_start.clone() else {
                    break;
                };
                fetch_objects_page(cursor, Some(next_start))?;
            }
            Ok(())
        };

        let jump_cursor_to =
            |cursor: &mut ObjectCursor, start: ListObjectsPageStart| -> Result<(), ServerError> {
                cursor.objects.clear();
                cursor.next_index = 0;
                cursor.next_page_start = Some(start);
                refill_cursor(cursor)
            };

        let skip_cursor_prefix =
            |cursor: &mut ObjectCursor, common_prefix: &str| -> Result<(), ServerError> {
                while cursor
                    .current()
                    .is_some_and(|obj| obj.key().as_str().starts_with(common_prefix))
                {
                    cursor.next_index += 1;
                    refill_cursor(cursor)?;
                }
                Ok(())
            };

        let mut cursors = Vec::new();
        self.pg_topology.for_each_pg(|pg_id| {
            let mut cursor = ObjectCursor {
                pg_id,
                objects: Vec::new(),
                next_index: 0,
                next_page_start: None,
            };
            fetch_objects_page(&mut cursor, initial_start.clone())?;
            cursors.push(cursor);
            Ok::<(), ServerError>(())
        })?;

        let max = max_keys as usize;
        let mut objects: Vec<ListEntry> = Vec::new();
        let mut common_prefixes: Vec<String> = Vec::new();
        let mut last_entry: Option<String> = None;
        let mut is_truncated = false;
        let mut active_common_prefix: Option<(String, Option<ObjectKey>)> = None;

        loop {
            let Some((cursor_index, current_key)) = cursors
                .iter()
                .enumerate()
                .filter_map(|(cursor_index, cursor)| {
                    cursor
                        .current()
                        .map(|object| (cursor_index, object.key().to_string()))
                })
                .min_by(|(left_index, left_key), (right_index, right_key)| {
                    left_key
                        .cmp(right_key)
                        .then_with(|| left_index.cmp(right_index))
                })
            else {
                break;
            };

            if let Some((ref common_prefix, ref upper_bound)) = active_common_prefix {
                if current_key.starts_with(common_prefix) {
                    if let Some(upper_bound) = upper_bound.clone() {
                        jump_cursor_to(
                            &mut cursors[cursor_index],
                            ListObjectsPageStart::At(upper_bound),
                        )?;
                    } else {
                        skip_cursor_prefix(&mut cursors[cursor_index], common_prefix)?;
                    }
                    continue;
                }
                active_common_prefix = None;
            }

            let current = cursors[cursor_index]
                .current()
                .expect("selected cursor should have a current object")
                .clone();
            let obj_key = current.key().to_string();
            if let Some(common_prefix_key) =
                storage::object_key_common_prefix(current.key(), prefix_str, delimiter)
            {
                let common_prefix = common_prefix_key.to_string();
                let upper_bound = storage::object_key_prefix_upper_bound(&common_prefix_key);
                active_common_prefix = Some((common_prefix.clone(), upper_bound));
                if objects.len() + common_prefixes.len() >= max {
                    is_truncated = true;
                    break;
                }
                common_prefixes.push(common_prefix.clone());
                last_entry = Some(common_prefix);
                continue;
            }

            if objects.len() + common_prefixes.len() >= max {
                is_truncated = true;
                break;
            }

            let record = current
                .as_live()
                .expect("list_objects returns only live objects");
            let system_metadata = self.deserialize_visible_system_metadata(
                record.system_metadata_blob.as_ref(),
                &record.encryption,
                None,
            )?;
            objects.push(ListEntry {
                key: obj_key.clone(),
                size: record.size,
                etag: record.etag.format(),
                last_modified: record.last_modified,
                checksum_algorithm: system_metadata.checksum_algorithm(),
                checksum_type: system_metadata.checksum_type(),
            });
            last_entry = Some(obj_key);
            cursors[cursor_index].next_index += 1;
            refill_cursor(&mut cursors[cursor_index])?;
        }

        let next_token = if is_truncated { last_entry } else { None };

        Ok(ListObjectsResult {
            objects,
            common_prefixes,
            is_truncated,
            next_continuation_token: next_token,
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
        let key_marker = req.key_marker;
        let version_id_marker = req.version_id_marker;
        let AuthorizedListObjectVersions { bucket_info } =
            self.authorize_list_object_versions(req)?;
        let owner_principal = bucket_info.owner_principal.clone();
        let owner_canonical_id = bucket_info.owner_canonical_id.clone();

        if max_keys == 0 {
            return Ok(ListObjectVersionsResult {
                versions: Vec::new(),
                is_truncated: false,
                next_key_marker: None,
                next_version_id_marker: None,
                owner_principal,
                owner_canonical_id,
            });
        }

        struct VersionCursor {
            versions: Vec<StoredObject>,
            next_index: usize,
        }

        impl VersionCursor {
            fn current(&self) -> Option<&StoredObject> {
                self.versions.get(self.next_index)
            }

            fn pop_current(&mut self) -> StoredObject {
                let version = self.versions[self.next_index].clone();
                self.next_index += 1;
                version
            }
        }

        let fetch_limit = max_keys.saturating_add(1);
        let list_prefix = optional_list_object_key(prefix)?;
        let list_key_marker = optional_list_object_key(key_marker)?;
        let mut cursors = Vec::new();
        self.pg_topology.for_each_pg(|pg_id| {
            let pg = self.storage_node.get_pg(pg_id)?;
            let resp = pg.list_object_versions(&ListObjectVersionsReq {
                bucket: bucket.clone(),
                prefix: list_prefix.clone(),
                key_marker: list_key_marker.clone(),
                version_id_marker,
                max_keys: fetch_limit,
            })?;
            cursors.push(VersionCursor {
                versions: resp.versions,
                next_index: 0,
            });
            Ok::<(), ServerError>(())
        })?;

        let max = max_keys as usize;
        let mut merged_versions = Vec::with_capacity(max.saturating_add(1));
        while merged_versions.len() <= max {
            let Some((cursor_index, _)) = cursors
                .iter()
                .enumerate()
                .filter_map(|(cursor_index, cursor)| {
                    cursor.current().map(|version| (cursor_index, version))
                })
                .min_by(|(left_index, left), (right_index, right)| {
                    left.key()
                        .cmp(right.key())
                        .then_with(|| left_index.cmp(right_index))
                })
            else {
                break;
            };
            merged_versions.push(cursors[cursor_index].pop_current());
        }

        let mut versions: Vec<VersionEntry> = Vec::new();
        let mut last_key: Option<&ObjectKey> = None;

        for obj in merged_versions.iter().take(max) {
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

        let is_truncated = merged_versions.len() > max;
        let (next_key_marker, next_version_id_marker) = if is_truncated {
            if let Some(last) = versions.last() {
                (Some(last.key.clone()), Some(last.version_id))
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };

        Ok(ListObjectVersionsResult {
            versions,
            is_truncated,
            next_key_marker,
            next_version_id_marker,
            owner_principal,
            owner_canonical_id,
        })
    }
}
